
import inspect, _ast

class Autoslots_meta(type):
    """
    Looks for assignments in __init__ and creates a __slot__ variable for all the
    instance attributes in the assignment. Detects assignments in __init__ of the form:

        self.attr = <...>

    """
    def __new__(cls, name, bases, dct):
        slots = dct.get('__slots__', [])
        orig_slots = []
        for base in bases:
            if hasattr(base, "__slots__"):
                orig_slots += base.__slots__

        if '__init__' in dct:
            init = dct['__init__']
            initproc = type.__new__(cls, name, bases, dct)
            initproc_source = inspect.getsource(initproc)
            ast = compile(initproc_source, "dont_care", 'exec', _ast.PyCF_ONLY_AST)
            classdef = ast.body[0]
            stmts = classdef.body
            for declaration in stmts:
                if isinstance(declaration, _ast.FunctionDef):
                    dname = declaration.name
                    if dname == '__init__':
                        initbody = declaration.body
                        for statement in initbody:
                            if isinstance(statement, _ast.Assign):
                                for target in statement.targets:
                                    if isinstance(target, _ast.Attribute) and \
                                            isinstance(target.value, _ast.Name) and \
                                            target.value.id == 'self':
                                        aname = target.attr
                                        if aname not in orig_slots:
                                            slots.append(aname)
            dct['__slots__'] = slots
        return type.__new__(cls, name, bases, dct)

class Autoslots(object):
    __metaclass__ = Autoslots_meta


__all__ = ['Autoslots', 'Autoslots_meta']


class TestClass(object):
    __metaclass__ = Autoslots_meta
    __slots__ = ['auto']
    auto = 'auto-value'

    def __init__(self):
        self.b = 2
        x = 10
        self.a = x

    def __str__(self):
        return "%s %s %s" % (self.a, self.b, self.auto)


if __name__ == '__main__':
    x = TestClass()
    x1 = TestClass()
    x2 = TestClass()
    x2.a = 10
    print x2
    x2.auto = 'auto-2'
    print x2



