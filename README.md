# FSHTBKVS

The FSHTBKVS (Filesystem Hash Table Based Key Value Store) was developed
to be an easy to deploy key value store with fast lookup times even for (very)
large data sets and without third party dependencies.

## Usage

### Basic
```python
from FSHTBKVS import FSHTBKVS

# create/open a kvs
kvs = FSHTBKVS(
  '/home/fshtbkvs/data',                                  # path where the kvs will be created/is located
  'Test_KVS'                                              # name of the kvs
)

kvs.write('this could be a key', 'this could be a value') # adds/updates a value
kvs.read('this could be a key')                           # returns the value for a given key
kvs.delete('this could be a key')                         # deletes a value
```

### Advanced
```python
kvs.export_kvs(file='/tmp/kvs_export.fshtbkvs')           # exports the whole kvs into a .fshtbkvs file
kvs.import_kvs(file='/tmp/kvs_export.fshtbkvs')           # imports a .fshtbkvs file

kvs.wipe_kvs()                                            # deletes all entries
```

### Miscellaneous
```python
kvs.get_entries()                                         # returns the number of entries
kvs.get_kvs_name()                                        # returns the name of the kvs
kvs.get_max_depth()                                       # returns the depth of the kvs
kvs.get_size_of_kvs()                                     # returns the size of the kvs in megabytes (as float)

kvs.maintain_kvs()                                        # recreates missing/broken .json files and counts all entries
```

## Information
When creating a KVS, the optional parameter 'max_depth' can be used.
It specifies how many levels of the hash table will be created.
The default value is 4, which results in 65,536 .json files. This seems to be
a sweet spot for most (hobby) projects. If you expect a very large amount of data,
the value should rather be set higher to keep the access times low. Naturally,
the higher the selected value, the more difficult it is to handle the KVS
(with regard to the file system).

| max_depth    | # of .json files created |
|--------------|-------------------------:|
| 1            | 16                       |
| 2            | 256                      |
| 3            | 4,096                    |
| **4**        | **65,536**               |
| 5            | 1,048,576                |
| 6            | 16,777,216               |
| 7            | 268,435,456              |
