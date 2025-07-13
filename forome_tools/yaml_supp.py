import yaml
from collections import Counter

#===============================================
def _str_presenter(dumper, data):
    """
    Preserve multiline strings when dumping yaml.
    https://github.com/yaml/pyyaml/issues/240
    """
    if "\n" in data:
        # Remove trailing spaces messing out the output.
        block = "\n".join([line.rstrip() for line in data.splitlines()])
        if data.endswith("\n"):
            block += "\n"
        return dumper.represent_scalar("tag:yaml.org,2002:str", block, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(str, _str_presenter)
yaml.representer.SafeRepresenter.add_representer(str, _str_presenter)

#===============================================
class YProperty:
    def __init__(self, name,
            value_type=str,
            required=False,
            is_seq=False,
            default=None):
        self.mName = name
        self.mType = value_type
        self.mIsRequired = required
        self.mIsSeq = is_seq
        self.mDefaultValue = default
        self.mConversion = None

    def getName(self):
        return self.mName

    def getType(self):
        return self.mType

    def isRequired(self):
        return self.mIsRequired

    def isSequence(self):
        return self.mIsSeq

    def setConversion(self, conversion_f):
        self.mConversion = conversion_f

    def _convValue(self, val,  check_it=True):
        if self.mType is None:
            return val
        if isinstance(self.mType, YClass):
            return self.mType.getValue(val)
        return self.mType(val)

    def getValue(self, obj, check_it=True):
        val = obj.get(self.mName)
        if val is None:
            val = self.mDefaultValue
        if self.mConversion is not None:
            val = self.mConversion(val)
        if val is None:
            assert not self.mIsRequired, (
                "Required y-property: " + self.mName + " in " + repr(obj))
            return None
        if self.mIsSeq:
            if isinstance(val,  list):
                return [self._convValue(v, check_it) for v in val]
            else:
                assert self.mType is str and isinstance(val, str), (
                    f"Y-property {self.mName}={val} should be a sequence")
                return val.split()
        return self._convValue(val, check_it)

#===============================================
class YClass:
    def __init__(self, properties):
        self.mProperties = properties
        cnt_names = Counter([prop.getName() for prop in self.mProperties])
        multiple_names = []
        for name, cnt in cnt_names.items():
            if cnt > 1:
                multiple_names.append(name)
        assert len(multiple_names) == 0, (
            "Multiple names for " + self.getClassName() + ": " +
            ", ".join(sorted(multiple_names)))

    def __iter__(self):
        return iter(self.mProperties)

    def getClassName(self):
        return self.mProperties[0].getName()

    def getValue(self, obj, check_it=True):
        ret = {}
        for prop in self.mProperties:
            val = prop.getValue(obj)
            if val is not None:
                ret[prop.getName()] = val
        if check_it:
            extra_keys = set(obj.keys()) - set(
                prop.getName() for prop in self.mProperties)
            assert len(extra_keys) == 0, (
                "Extra keys for " + self.getClassName() + ": " +
                ", ".join(sorted(extra_keys)))
        return ret

    def loadFile(self, fname, check_it=True):
        with open(fname, "r", encoding="utf-8") as inp:
            obj = yaml.load(inp.read(), yaml.Loader)
        return self.getValue(obj, check_it)

    def saveFile(self, fname, data):
        form_data = self.getValue(data)
        with open(fname, "w", encoding="utf-8") as outp:
            outp.write(yaml.dump(form_data, sort_keys=False, allow_unicode=True))
