def docstring_parameter(*sub,**kwargs):
    """ format doc string with params
        example:
        ```python
        @docstring_parameter('pos-arg-value',arg1='text', arg2='text')
        def func():
            '''
            {} - use for pos-arg-value
            text {arg1} {arg2}
            '''
        ```
    """
    def dec(obj):
        obj.__doc__ = obj.__doc__.format(*sub,**kwargs)
        return obj

    return dec