class DictObj:
    def __init__(self, attr=None):
        if attr is None:
            attr = {}
        # 确保所有字典都被封装为 DictObj
        for key, value in attr.items():
            if isinstance(value, dict):
                attr[key] = DictObj(value)
        self._attributes = attr

    def get(self, key, default=None):
        return self._attributes.get(key, default)
        
    def __iter__(self):
        return iter(self._attributes)
        
    def __getitem__(self, key):
        return self._attributes[key]

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = DictObj(value)
        self._attributes[key] = value

    def __getattr__(self, name):
        try:
            return self._attributes[name]
        except KeyError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name == "_attributes":
            super().__setattr__(name, value)
        else:
            if isinstance(value, dict):
                value = DictObj(value)
            self._attributes[name] = value

    def __contains__(self, key):
        return key in self._attributes
        
    def keys(self):
        return self._attributes.keys()

    def values(self):
        return self._attributes.values()

    def __len__(self):
        return len(self._attributes)

    def items(self):
        return self._attributes.items()        
        
    def __str__(self):
        attributes = ', '.join(f"{key}={value!r}" for key, value in self._attributes.items())
        return f"{type(self).__name__}({attributes})"
    
    def __delitem__(self, key):
        del self._attributes[key]