from third_party.promise import Promise, promisify


class CachingLoader(object):
    """An object to be used for caching asynchronous service/function calls identifiable by a key.

        More specifically, server applications often need to make the same service calls in
        independent parts of the application with the lifetime of a single request. To make it
        simple to keep these modules decoupled but still allow for optimal service calls this
        caching loader is used to wrap around the actual interface and handle the caching
        transparently to the caller.
    """

    def __init__(self, load_fn, cache_map=None, cache_key_fn=None):
        assert callable(load_fn), (
            'CachingLoader must be constructed with a function which accepts a given key and '
            'returns a future or a promise, but got: %s.' % type(load_fn))

        self._load_fn = load_fn
        self._promise_cache = cache_map if cache_map is not None else {}
        self._cache_key_fn = cache_key_fn or (lambda x: x)

    def load(self, key):
        """Loads a key, returning a `Promise` for the value represented by that key."""
        cache_key = self._cache_key_fn(key)
        if cache_key in self._promise_cache:
            return self._promise_cache[cache_key]
        else:

            def handle_load_call(resolve, reject):
                loaded_promise = promisify(self._load_fn(key))

                def handle_error(error):
                    self._failed_dispatch(key, reject, error)

                loaded_promise.then(resolve).catch(handle_error)

            # We have to store an empty promise in the cache, since the promise .then and .catch
            # functions can be resolved synchronously if the result has already resolved.
            # Then after the promise has been cached we modify that existing object and handle
            # the load call.
            # The reason we do this, is so we can ensure that the promise in the cache is exactly
            # the same promise as returned by the load call. That allows to compare the returned
            # Promises for equality rather than having to resolve them first.
            promise = Promise()
            self._promise_cache[cache_key] = promise
            promise.do_resolve(handle_load_call)

            return promise

    def clear(self, key):
        """Clears the value at `key` from the cache, if it exists.

        Returns self for method chaining.
        """
        cache_key = self._cache_key_fn(key)
        del self._promise_cache[cache_key]
        return self

    def clear_all(self):
        """Clears the entire cache. Returns self for method chaining."""
        self._promise_cache = {}
        return self

    def prime(self, key, promise):
        """Adds the provided key and promise to the cache.

        If the key already exists, no change is made. Returns self for method chaining.
        """

        assert hasattr(promise, 'then') and callable(promise.then), (
            "Expected a promise to be passed in (which implements a 'then' function), but "
            "received %s" % type(promise))

        cache_key = self._cache_key_fn(key)

        if cache_key not in self._promise_cache:
            self._promise_cache[cache_key] = promise

        return self

    def _failed_dispatch(self, key, reject, error):
        self.clear(key)
        reject(error)
