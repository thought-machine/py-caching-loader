import unittest
from concurrent.futures import Future

from third_party.promise import Promise

from caching_loader.caching_loader import CachingLoader


def _key_loader_count(cache_map=None, cache_key_fn=None):
    load_calls = []

    def _key_loader_fn(key):
        load_calls.append(key)
        return Promise.resolve(key)

    key_loader = CachingLoader(_key_loader_fn, cache_map, cache_key_fn)

    return key_loader, load_calls


def _stringify_cache_key_fn(proto):
    return hash(str(proto))


class SingleElementDict(dict):
    def __init__(self, *arg, **kw):
        super(SingleElementDict, self).__init__(*arg, **kw)

    def __setitem__(self, key, item):
        assert len(self.__dict__.keys()) < 1, ('Cannot set key %s. This dict can only hold one '
                                               'item at a time' % key)
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __contains__(self, key):
        try:
            # pylint: disable=pointless-statement
            self.__dict__[key]
            return True
        except KeyError:
            return False


class CachingLoaderLoadTests(unittest.TestCase):

    def test_loading_a_key_returns_a_promise(self):
        key_loader = CachingLoader(Promise.resolve)
        result = key_loader.load('some-key')
        self.assertIsInstance(result, Promise)

    def test_loading_a_key_resolves_to_the_correct_value(self):
        key_loader = CachingLoader(Promise.resolve)
        result = key_loader.load('another-key').get()
        self.assertEquals(result, 'another-key')

    def test_loading_different_keys_returns_different_promises(self):
        key_loader = CachingLoader(Promise.resolve)

        result1 = key_loader.load('some-key')
        result2 = key_loader.load('another-key')

        self.assertNotEquals(result1, result2)

    def test_loading_different_keys_returns_correct_values(self):
        key_loader = CachingLoader(Promise.resolve)

        result1 = key_loader.load('some-key').get()
        result2 = key_loader.load('another-key').get()

        self.assertEquals(result1, 'some-key')
        self.assertEquals(result2, 'another-key')

    def test_loading_a_key_multiple_times_returns_cached_promise_and_results(self):
        key_loader = CachingLoader(Promise.resolve)

        result1 = key_loader.load('some-key')
        result2 = key_loader.load('some-key')

        self.assertEquals(result1, result2)
        self.assertEquals(result1.get(), result2.get())

    def test_loading_a_key_multiple_times_calls_loader_fn_only_once(self):
        key_loader, load_calls = _key_loader_count()

        key_loader.load('some-key')
        key_loader.load('some-key')

        self.assertEquals(len(load_calls), 1)

    def test_loading_a_key_that_returns_errors_returns_promise(self):
        key_loader = CachingLoader(lambda key: Promise.rejected(Exception(key)))
        result = key_loader.load('some-key')
        self.assertIsInstance(result, Promise)

    def test_loading_a_key_that_resolves_to_error_gets_propagated(self):
        key_loader = CachingLoader(lambda key: Promise.rejected(Exception(key)))
        with self.assertRaises(Exception) as ctx:
            key_loader.load('some-key').get()

        self.assertEquals(str(ctx.exception), 'some-key')

    def test_loading_a_key_that_resolves_to_error_removes_that_entry_from_cache(self):
        key_loader = CachingLoader(lambda key: Promise.rejected(Exception(key)))

        result1 = key_loader.load(1)
        result2 = key_loader.load(1)

        # Since the loading function here returns a 'synchronous' rejected promise it will not
        # clear itself off the cache immediately.
        self.assertNotEquals(result1, result2)

        with self.assertRaises(Exception):
            result1.get()

        result3 = key_loader.load(1)

        self.assertNotEquals(result1, result3)

    def test_loading_a_key_that_resolves_to_a_future_gets_converted_to_promise(self):
        f = Future()
        f.set_result(1)
        future_loader = CachingLoader(lambda key: f)

        result1 = future_loader.load(1)

        self.assertIsInstance(result1, Promise)
        self.assertEquals(result1.get(), 1)


class CachingLoaderClearTests(unittest.TestCase):

    def test_can_clear_entry_from_cache(self):
        key_loader, load_calls = _key_loader_count()

        result1 = key_loader.load(1)
        key_loader.clear(1)
        result2 = key_loader.load(2)

        self.assertNotEquals(result1, result2)
        self.assertEquals(len(load_calls), 2)

    def test_can_clear_all_entries_from_cache(self):
        key_loader, load_calls = _key_loader_count()

        result1 = key_loader.load(1)
        result2 = key_loader.load(2)
        result3 = key_loader.load(3)

        key_loader.clear_all()

        result4 = key_loader.load(1)
        result5 = key_loader.load(2)
        result6 = key_loader.load(3)

        self.assertNotEquals(result1, result4)
        self.assertNotEquals(result2, result5)
        self.assertNotEquals(result3, result6)
        self.assertEquals(len(load_calls), 6)


class CachingLoaderPrimeTests(unittest.TestCase):

    def test_prime_asserts_that_promise_like_element_is_passed_in(self):
        key_loader, _ = _key_loader_count()
        with self.assertRaises(AssertionError) as ctx:
            key_loader.prime('some-key', 'hello')

        self.assertEquals(str(ctx.exception), 'Expected a promise to be passed in (which '
                                              'implements a \'then\' function), but received '
                                              "<class 'str'>")

    def test_prime_puts_element_into_cache_without_calling_load_fn(self):
        key_loader, load_calls = _key_loader_count()
        key_loader.prime('some-key', Promise.resolve('hello'))
        self.assertEquals(len(load_calls), 0)

    def test_can_preload_value_into_cache_if_key_is_free(self):
        key_loader, load_calls = _key_loader_count()
        result1 = key_loader.prime('some-key', Promise.resolve('hello')).load('some-key')

        self.assertEquals(len(load_calls), 0)
        self.assertIsInstance(result1, Promise)
        self.assertEquals(result1.get(), 'hello')

    def test_prime_does_nothing_if_key_is_already_populated(self):
        key_loader, _ = _key_loader_count()

        result1 = key_loader.load('some-key')
        result2 = key_loader.prime('some-key', Promise.resolve('great new value')).load('some-key')

        self.assertEquals(result1, result2)
        self.assertEquals(result1.get(), result2.get())

    def test_can_preload_exception_into_cache_if_key_is_free(self):
        key_loader, _ = _key_loader_count()

        result1 = key_loader.prime(
            'some-key', Promise.rejected(Exception('Why would you do that?'))).load('some-key')

        self.assertIsInstance(result1, Promise)

        with self.assertRaises(Exception) as ctx:
            result1.get()

        self.assertEquals(str(ctx.exception), 'Why would you do that?')


class CachingLoaderOptionsTests(unittest.TestCase):

    def test_custom_cache_key_function(self):
        key_loader, load_calls = _key_loader_count(cache_key_fn=_stringify_cache_key_fn)

        class Tea(object):
            def __init__(self, name, age, category):
                self.name = name
                self.age = age
                self.category = category

        request = Tea(name='Oolong', age=1, category='GREEN')
        response1 = key_loader.load(request)

        self.assertIsInstance(response1, Promise)
        self.assertEquals(response1.get(), request)

        response2 = key_loader.load(request)

        self.assertEquals(response1, response2)
        self.assertEquals(response1.get(), response2.get())
        self.assertEquals(len(load_calls), 1)

        request2 = Tea(name='Silver White', age=1, category='WHITE')
        response3 = key_loader.load(request2)

        self.assertNotEquals(response1, response3)
        self.assertNotEquals(response1.get(), response3.get())
        self.assertEquals(len(load_calls), 2)

    def test_accepts_custom_cache_map(self):
        key_loader, load_calls = _key_loader_count(cache_map=SingleElementDict())

        response1 = key_loader.load('some-key')

        self.assertIsInstance(response1, Promise)
        self.assertEquals(response1.get(), 'some-key')

        response2 = key_loader.load('some-key')
        self.assertEquals(response1, response2)
        self.assertEquals(len(load_calls), 1)

        with self.assertRaises(AssertionError) as ctx:
            key_loader.load('other-key')

        self.assertEquals(str(ctx.exception), 'Cannot set key other-key. This dict can only hold '
                                              'one item at a time')
