"""Key-value store using Pyarrow's plasma object store as backend"""

import functools
import os
import subprocess
import time
from hashlib import blake2b
from humanfriendly import format_size
from pyarrow import plasma


# Decorators
def check_connected(func):
    """Check if the plasma object store is connected"""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._connected:
            return func(self, *args, **kwargs)
        else:
            raise ConnectionError(
                "Plasma object store is not connected. Use connect() to connect to it first"
            )

    return wrapper


class PyArrowPlasmaKVStore:
    """Key-value store using Pyarrow's plasma object store as its backend

    Plasma object store:
      1. Plasma, an in-memory object store
      2. Holds immutable objects in shared memory so that they can be accessed efficiently \
          by multiple processes
      3. Enables zero-copy data exchange between processes (I am really not sure about this, \
          as documentation to achieve this is not quite clear)

    Args:
      path: Path to the plasma object store socket file (e.g., /tmp/plasma)
      name: Name of the plasma object store socket file (default: pykvstores_plasma_store_socket)
      size: Size of the plasma object store in bytes (default: 70% of the available shared memory)
      create: Create the plasma object store and the socket file (default: True)

    Notes:
      1. Keys are hashed using BLAKE2 algorithm and the resulting 20 bytes hash digest is used as \
          the ObjectID
      2. If the socket file already exists, it is deleted and a new socket file is created for the \
          new instance
      3. The plasma object store is started in the background using subprocess.Popen, and the \
          process is terminated when the cleanup() method is called
      4. Do not forget to call the cleanup() method when the PlasmaKVStore object store is \
          no longer needed
      5. Safe to read/write in multiple processes
      6. Only one instance of the plasma object store can be created per socket file

    """

    def __init__(
        self, path, name="pykvstores_plasma_store_socket", size=None, create=True
    ):
        # Set the socket path. If path does not exist create it
        self.socket_path = os.path.join(path, name)
        if not os.path.exists(path):
            os.makedirs(path)

        self.size = self.__check_size(size)
        self.__created = False
        self.plasma_store_proc = None
        self.client = None
        self._connected = False

        # Create the plasma object store
        if create:
            self.create_plasma_store()

    @check_connected
    def __setitem__(self, key, value):
        """Set an item in the plasma object store

        Args:
          key: Key (type: string, bytes, or int). The key, value pair is \
              ignored if the key already exists.
          value: Value (Any python object)

        """
        # Generate an object ID
        object_id = self.gen_object_id(key)

        # Set the key-value pair in the plasma object store
        try:
            self.client.put([key, value], object_id)
        except plasma.PlasmaObjectExists:
            pass
        except plasma.PlasmaStoreFull:
            raise ValueError(
                "Plasma object store is full. Please free some memory by deleting some objects"
            ) from plasma.PlasmaStoreFull

    @check_connected
    def __getitem__(self, key):
        """Get an item from the plasma object store

        Args:
          key: Key (type: string, bytes, or int)

        Returns: Value if present, None otherwise

        """
        # Generate an object ID
        object_id = self.gen_object_id(key)

        # Get the value from the plasma object store
        try:
            return self.client.get(object_id)[1]
        except plasma.PlasmaObjectNotFound:
            return None

    @check_connected
    def __delitem__(self, keys):
        """Delete one or more items from the plasma object store

        Args:
            keys: A single list of keys (type: string, bytes, or int)

        """
        # Generate an object ID
        if isinstance(keys, list):
            object_ids = [self.gen_object_id(key) for key in keys]
        else:
            raise TypeError("keys must be a list")

        # Delete the object from the plasma object store
        self.client.delete(object_ids)

    @check_connected
    def __contains__(self, key):
        """Check if the key exists in the plasma object store

        Args:
          key: Key (type: string, bytes, or int)

        Returns: True if the key exists, False otherwise

        """
        # Generate an object ID
        object_id = self.gen_object_id(key)

        # Check if the object exists in the plasma object store
        return self.client.contains(object_id)

    @check_connected
    def __iter__(self):
        """Iterate over all keys and values"""
        # Get all the object IDs
        object_ids = self.client.list()

        for key, value in self.client.get(list(object_ids.keys())):
            yield key, value

    @check_connected
    def __len__(self):
        """Get the number of items in the plasma object store

        Returns: Number of items in the plasma object store

        """
        return len(self.client.list())

    @check_connected
    def keys(self):
        """Get all the keys in the plasma object store

        Returns: List of keys

        """
        # local keys list
        keys = []

        # Get all the object IDs
        object_ids = self.client.list()

        # Get all the values
        _ = [keys.append(k) for k, _ in self.client.get(list(object_ids.keys()))]

        return keys

    @check_connected
    def values(self):
        """Get all the values in the plasma object store

        Returns: List of values

        """
        # local values list
        values = []

        # Get all the object IDs
        object_ids = self.client.list()

        # Get all the values
        _ = [values.append(v) for _, v in self.client.get(list(object_ids.keys()))]

        return values

    @check_connected
    def replace(self, key, value):
        """Replace an item in the plasma object store

        If key does not exist, it is added to the plasma object store

        Args:
          key: Key (type: string, bytes, or int)
          value: Value (Any python object)

        """
        # Generate an object ID
        object_id = self.gen_object_id(key)

        # Delete the object from the plasma object store if it exists and add the new value
        try:
            self.client.delete([object_id])
            self.client.put([key, value], object_id)
        except plasma.PlasmaStoreFull:
            raise ValueError(
                "Plasma object store is full. Please free some memory by deleting some objects"
            ) from plasma.PlasmaStoreFull

    def delete(self, keys):
        """Delete one or more items from the plasma object store

        Args:
          keys: Single key or a single list of keys (type: string, bytes, or int)

        """
        if isinstance(keys, list):
            self.__delitem__(keys)
        else:
            self.__delitem__([keys])

    def get_multi(self, keys):
        """Get multiple items from the plasma object store

        Args:
          keys: List of keys (type: string, bytes, or int)

        Returns: Dictionary of key-value pairs. If key is not present in the DB, \
            it is not present in the returned dictionary

        """
        local_dict = {}

        for key in keys:
            local_dict[key] = self[key]

        return local_dict

    def set_multi(self, keys, values):
        """Set multiple items in the plasma object store

        Args:
          keys: List of keys (type: string, bytes, or int). Duplicate keys will be overwritten
          values: List of values (Any Python object).

        """
        if len(keys) != len(values):
            raise ValueError("Keys and values should be of same length")

        for key, value in zip(keys, values):
            self[key] = value

    def create_plasma_store(self):
        """Create the plasma object store"""
        # Create the plasma object store
        if not self.__created:
            self.plasma_store_proc = self.__initialize_plasma_store_in_background()
            self.__created = True
        else:
            raise RuntimeError(
                "Plasma object store already created. Use connect() instead of \
                    create_plasma_store()"
            )

    def connect(self):
        """Connect to the plasma object store"""
        # Try connecting to the plasma object store and if it fails,
        # terminate the plasma object store and raise a connection error
        if self.__created:
            try:
                self.client = plasma.connect(self.socket_path)
                self._connected = True
            except Exception as excep:
                self.cleanup()
                raise ConnectionError(
                    "Failed to connect to the plasma object store"
                ) from excep
        else:
            raise ConnectionError(
                "Plasma object store has not been created. Use create_plasma_store() \
                    to create it first"
            )

    @check_connected
    def disconnect(self):
        """Disconnect from the plasma object store"""
        # Disconnect this client from the plasma object store
        try:
            self.client.disconnect()
            self._connected = False
        except Exception as excep:
            raise ConnectionError(
                "Failed to disconnect from the plasma object store"
            ) from excep

    def gen_object_id(self, key):
        """Generate an object ID

        Args:
            key: Key

        Returns: Object ID

        """
        # Generate a 20 bytes hash digest of the key
        key_digest = self.gen_hash(key)

        # Use the hash digest as the object ID
        return plasma.ObjectID(key_digest)

    @check_connected
    def get_object_ids(self):
        """Get all the object IDs in the plasma object store

        Returns: List of object IDs

        """
        return list(self.client.list().keys())

    @check_connected
    def get_available_memory(self, human_readable=True):
        """Get the available free memory in the plasma object store

        Args:
          human_readable: If True, return the memory usage in human readable format

        Returns: Available free memory in the plasma object store

        """
        capacity = self.client.store_capacity()
        used = self.get_used_memory(human_readable=False)

        if human_readable:
            return format_size(capacity - used)
        else:
            return capacity - used

    @check_connected
    def get_used_memory(self, human_readable=True):
        """Calculate the amount of memory used in the plasma object store

        Args:
          human_readable: If True, return the memory usage in human readable format

        Returns: Amount of total used memory by the objects in the plasma object store

        """
        used = 0
        objects_list = self.client.list()

        for _, metadata in objects_list.items():
            used += metadata["data_size"] + metadata["metadata_size"]

        if human_readable:
            return format_size(used)
        else:
            return used

    @check_connected
    def get_store_capacity(self, human_readable=True):
        """Get the total capacity of the plasma object store

        Args:
          human_readable: If True, return the memory usage in human readable format

        Returns: Storage/memory capacity of the plasma object store

        """
        capacity = self.client.store_capacity()

        if human_readable:
            return format_size(capacity)
        else:
            return capacity

    def cleanup(self):
        """Terminate the plasma_store process and clean up the socket file"""
        if self.__created:
            self.plasma_store_proc.terminate()
            self.plasma_store_proc.wait()
            self.plasma_store_proc = None
            self.__created = False

            # Remove the socket file
            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)
        else:
            raise RuntimeError(
                "Plasma object store has not been created. Use create_plasma_store() \
                    to create it first"
            )

    def __initialize_plasma_store_in_background(self):
        """Start the plasma store in the background using subprocess"""
        # Delete the socket if it already exists for new instances
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

        plasma_store_process = subprocess.Popen(
            ["plasma_store", "-s", self.socket_path, "-m", str(self.size)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(0.2)

        # Check if plasma store started successfully
        if plasma_store_process.poll() is not None:
            raise RuntimeError(
                f"plasma_store failed to start with return code: {plasma_store_process.returncode}"
            )

        return plasma_store_process

    def __check_size(self, size):
        """Check if the size is valid

        Args:
          size: Size of the plasma object store in bytes

        Returns: Size of the plasma object store in bytes

        Notes:
          1. If the requested size is larger than the maximum allowed size \
              (100% of the shared memory), raise an exception
          2. If no size is specified, use the default size (70% of the available shared memory size)

        """
        # Refer statvfs for more details
        # f_bsize == File system block size,
        # f_bavail == Number of free blocks for unpriviledged users
        # Example: df -h /dev/shm | awk '{print $4}' (prints the available memory \
        # in the shared memory)
        available_shm_mem = (
            os.statvfs("/dev/shm").f_bsize * os.statvfs("/dev/shm").f_bavail
        )
        if size:
            if size <= available_shm_mem:
                return int(size)
            else:
                raise ValueError(
                    "The size of the plasma object store is larger than the available \
                        shared memory space."
                )
        else:
            return int(available_shm_mem * (70 / 100))

    @staticmethod
    def gen_hash(key, digest_size=20):
        """Generate the hash of a key using hashlibs BLAKE2 algorithm

        BLAKE2 seems to be a good choice for hashing compared to SHA-256, SHA-512, SHA-3, etc.
        BLAKE3 is still not available in hashlib and as soon as it is available we will use it.

        Args:
          key: Key (string, bytes, int)
          digest_size: Size of the digest (default: 20 bytes)

        Returns: Hash digest of the key

        """
        if isinstance(key, str):
            hash_value = blake2b(key.encode(), digest_size=digest_size)
        if isinstance(key, bytes):
            hash_value = blake2b(key, digest_size=digest_size)
        if isinstance(key, int):
            hash_value = blake2b(str(key).encode(), digest_size=digest_size)
        return hash_value.digest()
