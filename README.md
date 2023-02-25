# On Disk Key Value 

### Features

- Store keys and values on-disk, keeping an in-memory map of where a key's value is located in the db file
- Background thread to compact log
- Uses GOB to encode which is unnecessary for strings but should support any go struct (future usecase)