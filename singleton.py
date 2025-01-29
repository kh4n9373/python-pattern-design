class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class Singleton(metaclass=SingletonMeta):
    def some_business_logic(self):
        print('hi')


if __name__ == "__main__":
    s1 = Singleton()
    s2 = Singleton()

    if (s1 is s2):
        print("singleton true")
    else:
        print("singleton failed")