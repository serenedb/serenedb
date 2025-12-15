import lldb

def class_with_toString(valobj, internal_dict, options):
    # assign the object to v
    if valobj.TypeIsPointerType():
        v = valobj.Dereference()
        if not v.GetAddress().IsValid():
            return "NULL"
    else:
        v = valobj
    s = v.GetNonSyntheticValue().EvaluateExpression('toString()').GetSummary()
    return "" if s is None else str(s)
