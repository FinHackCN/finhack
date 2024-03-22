class DictObj:
    def __init__(self, attr=None):
        if attr is None:
            attr = {}
        # 确保所有字典都被封装为 DictObj
        for key, value in attr.items():
            if isinstance(value, dict):
                attr[key] = DictObj(value)
        self._attributes = attr


    def __getstate__(self):
        # 在序列化时调用，返回对象的状态
        # 将所有 DictObj 实例转换为普通字典
        state = self._attributes.copy()
        for key, value in state.items():
            if isinstance(value, DictObj):
                state[key] = value.__getstate__()  # 递归调用以处理嵌套的 DictObj
        return state

    def __setstate__(self, state):
        # 在反序列化时调用，使用保存的状态重新构建对象
        # 将所有普通字典转换回 DictObj 实例
        for key, value in state.items():
            if isinstance(value, dict):
                state[key] = DictObj(value)  # 递归调用以处理嵌套的字典
        self._attributes = state


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
        
    def push(self, key, value):
        """添加或更新字典中的键值对。"""
        self[key] = value

    def pop(self, key, default=None):
        """从字典中移除指定的键并返回其值，如果键不存在，则返回default。"""
        return self._attributes.pop(key, default)