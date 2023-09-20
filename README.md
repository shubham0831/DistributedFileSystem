# Distributed File System

### How to run

1. Log on to orion
2. Start the controller using the following command :

```jsx
// cd into the directory where the database is
cd /bigdata/spareek/project/P2-shubham0831/
// if you want a fresh table
/home/spareek/go/bin/server -la orionXX.cs.usfca.edu:PORT -db ./server/db.db -freshTable true
// if you don't want a fresh table
/home/spareek/go/bin/server -la orionXX.cs.usfca.edu:PORT -db ./server/db.db -freshTable true
```

3. Start the cluster (Ideally from a different tab)

```jsx
// cd into the correct directory
cd /bigdata/spareek/project/P2-shubham0831/scripts
// start the cluster
./start-cluster.sh
```

4. Ssh to where you want to run the client, then run the client

```jsx
// -sa is the address of the server
/home/spareek/go/bin/client -sa orionXX.cs.usfca.edu:PORT
```

### Supported Commands

All the commands except cd and shownodes have an additional parameter at the end where the user can specify the path to the directory where the operation is supposed to be carried out. This path is always relative to the current working directory of the user.

For example for the LS command, the user can either just type in “ls” or they can type in     “ls ../somedir” which will list all files in somedir

| Commands | Format | Description |
| --- | --- | --- |
| LS | ls | Shows all files and folders in directory of choice. |
| MKDIR | mkdir {dir_name} | Creates a folder in your current directory or directory of choice.  |
| PWD | pwd | Prints the full path of the current working directory or a directory of your choice. |
| CD | cd {path} | Changes the users current directory. The user can either specify a full path, or a relative path |
| Share | share {path_to_file} | Share a file of the users choice to the dfs |
| Get | get {path_to_file} | Get a file from the dfs  |
| Delete | del {file_name} | Delete a file. Does not support deleting a folder |
| ShowNodes | shownodes | Shows information about all the active nodes |

### High Level Design

Node Registration

![image](https://user-images.githubusercontent.com/48670085/197638069-702fa22d-dda7-4391-8b04-d658d90dbbec.png)


Commands except share and get are basically just the server pulling up data from the database and sending over the response to the client

Share File

![image](https://user-images.githubusercontent.com/48670085/197638116-e2871b08-f04d-4ede-bb95-a43fda583d7b.png)


Get File

![image](https://user-images.githubusercontent.com/48670085/197638152-81d4fa99-0132-43f4-95b6-0dbdc864fb5d.png)

Map Reduce 

![mapReduce](https://user-images.githubusercontent.com/48670085/206827510-13205356-d91b-45c6-b041-8e5368d2e904.png)


