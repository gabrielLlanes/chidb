CK_RUN_CASE="Step 1a: Opening an existing chidb file" make check
CK_RUN_CASE="Step 1b: Opening a new chidb file" make check
CK_RUN_CASE="Step 2: Loading a B-Tree node from the file" make check
CK_RUN_CASE="Step 3: Creating and writing a B-Tree node to disk" make check
CK_RUN_CASE="Step 4: Manipulating B-Tree cells" make check
CK_RUN_CASE="Step 5: Finding a value in a B-Tree" make check
CK_RUN_CASE="Step 6: Insertion into a leaf without splitting" make check
CK_RUN_CASE="Step 7: Insertion with splitting" make check
CK_RUN_CASE="Step 8: Supporting index B-Trees" make check