"""Key-value store using Ray's actors and Ray's plasma object store as backend"""

import ray


@ray.remote
class RayKVActor:
    """Ray key-value store actor"""

    def __init__(self):
        # Stores the keys and the ObjectRef's
        self.kvstore = {}

    def set(self, key, value):
        """Set an item in the Ray's plasma object store

        Args:
          key: The key to set
          value: The value to set

        """
        self.kvstore[key] = ray.put(value)

    def get(self, key):
        """Get an item from the Ray's plasma object store

        Args:
          key: The key to get, if it exists else None

        """
        try:
            return ray.get(self.kvstore[key])
        except KeyError:
            return None

    def len(self):
        """Return the number of items in the Ray's plasma object store"""
        return len(self.kvstore)

    def contains(self, key):
        """Return whether the key is in the Ray's plasma object store

        Args:
          key: The key to check

        """
        return key in self.kvstore

    def __delitem__(self, key):
        """Delete an item from the Ray's plasma object store

        Args:
          key: The key to delete

        """
        try:
            del self.kvstore[key]
        except KeyError:
            pass

    def iter(self):
        """Return an iterator over the keys in the Ray's plasma object store"""
        return self.kvstore.items()

    def keys(self):
        """Return a list of keys in the Ray's plasma object store"""
        return list(self.kvstore.keys())

    def values(self):
        """Return a list of values in the Ray's plasma object store"""
        return ray.get(list(self.kvstore.values()))

    def get_multi(self, keys):
        """Return multiple items from the Ray's plasma object store

        Args:
          keys: The keys to get

        """
        return [ray.get(self.kvstore[key]) for key in keys]

    def set_multi(self, keys, values):
        """Set multiple items in the Ray's plasma object store

        Args:
          keys: List of keys
          values: List of values

        """
        for key, value in zip(keys, values):
            self.kvstore[key] = ray.put(value)

    def getattr(self, attr):
        """Return the value of the attribute

        Args:
          attr: The attribute to get

        """
        return getattr(self, attr)


class RayKVStore:
    """Key-value store using Ray actors and Ray's plasma object store as backend

    Plasma object store:
      1. Plasma, an in-memory object store
      2. Holds immutable objects in shared memory so that they can be accessed efficiently \
          by multiple processes
      3. Enables zero-copy data exchange between processes (I am really not sure about this, \
          as documentation to achieve this is not quite clear)

    Args:
      ncpus: To initialize the Ray cluster (default: 1)

    Notes:
      1. Safe to read/write in multiple processes
      2. Only one instance of the RayKVStore class can be created as the Ray cluster is \
            initialized in the constructor
      3. Do not forget to call the close() method to close the Ray cluster

    """

    def __init__(self, ncpus=1):
        ray.init(
            num_cpus=ncpus,
            num_gpus=0,
            include_dashboard=False,
            ignore_reinit_error=True,
        )

        # Create the actor
        self.actor = RayKVActor.remote()

    def __setitem__(self, key, value):
        """Set an item in the Ray's plasma object store

        Args:
          key: Key (is overwritten if the key already exists)
          value: The value to be set

        """
        self.actor.set.remote(key, value)

    def __getitem__(self, key):
        """Get an item from the Ray's plasma object store

        Args:
          key: The key to be retrieved
        """
        return ray.get(self.actor.get.remote(key))

    def __delitem__(self, key):
        """Delete an item from the Ray's plasma object store

        Args:
          key: The key to be deleted

        """
        self.actor.__delitem__.remote(key)

    def __contains__(self, key):
        """Check if an item is in the Ray's plasma object store

        Args:
          key: The key to be checked

        """
        return ray.get(self.actor.contains.remote(key))

    def __len__(self):
        """Return the number of items in the Ray's plasma object store"""
        return ray.get(self.actor.len.remote())

    def __iter__(self):
        """Return an iterator over the items in the Ray's plasma object store"""
        items = ray.get(self.actor.iter.remote())
        for key, value in items:
            yield key, ray.get(value)

    def keys(self):
        """Return the keys in the Ray's plasma object store"""
        return ray.get(self.actor.keys.remote())

    def values(self):
        """Return the values in the Ray's plasma object store"""
        return ray.get(self.actor.values.remote())

    def get_multi(self, keys):
        """Return the values in the Ray's plasma object store

        Args:
          keys: List of keys to be retrieved

        """
        return ray.get(self.actor.get_multi.remote(keys))

    def set_multi(self, keys, values):
        """Set multiple items in the Ray's plasma object store

        Args:
          keys: List of keys
          values: List of values

        """
        if len(keys) != len(values):
            raise ValueError("Keys and values must be of the same length")

        self.actor.set_multi.remote(keys, values)

    def close(self):
        """Shutdown the Ray cluster"""
        ray.shutdown()
