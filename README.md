# eplusparser

A python package to read EnergyPlus `eplusout.sql` files into
[pandas DataFrames](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

## Install

`python setup.py install`

## Example

```python
from eplusparser import parse
df = parse('eplusout.sql')
```
