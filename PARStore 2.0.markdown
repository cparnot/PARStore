## PARStore 2.0

Outline:

1. Get rid of CoreData --> just SQLite (+ FMDB or similar).
2. Do not keep the database open, except maybe the r/w for a short amount of time to optimize for writing bursts.
3. Always read the entire database when opening, no need for `relevantKeysForSync` (this is already in place as of commit b79be6ab2e8); this was an unnecessary premature optimization.
4. Swift-only, or at least, swift-aware (PARStore 1 is already compatible with swift, see commit series ending with 62375f9352ee9fc1b5b2609157e576a19abe2f58); the PARDispatchQueue dependency may also be updated to swift.
5. Simplify the caching: memory layer is still a dictionary (with the latest values for each key), but we also need to cache some of the database information, and we might as well just cache the relevant rows instead of all the complicated setup of PARStore 1. Most of that complexity is historical: as I built more functionality into PARStore, I added some way to keep track of more information as part of the memory cache, and ended up storing pretty much the same information as what's in the database queue, but in a different structure.

The problem with (1) is that it will likely break backward-compatibility (none of the other points do). Maybe we can be careful to maintain some kind of CoreData-compatible structure using the right SQLite setup, or else officially break backward-compatibility. At least, we should be able to maintain backward-compatibility for files created with PARStore 1, and manipulated by PARStore 2. We could also implement both and have a compatibility flag. Given the simplified cache setup outlined in (5), it would not be very hard. In any case, we want to be forward-compatible and be able to open stores created with PARStore 1, so some of this will have to be implemented.

The idea with (5) is simply to keep a proper cache of the relevant database rows, and pass those back and forth between memory and database. The logs cache being part of the memory queue means we can do all the translation between the logs and the KV dictionary within the memory queue. The database queue is relegated to a simple role of pushing values into the local database, and pulling values out of the foreign databases, keeping track of where it started last. This would remove all the state management used to keep track of the different device latest updates.

The different elements are represented in the diagram below:

1. KV dictionary. This is the 'truth' exposed to the clients. Clients can get and set KV pairs from any thread. Internally, this sets/gets value on the KV dictionary and access is serialized by the memory queue.
2. Local database. Access is serialized via the database queue. This is the only database the store can write to. It is also never read except with the initial loading (see below).
3. Foreign databases. Access is serialized via the database queue. These databases are read-only. The queue remembers the last row read for each of the foreign dbs, so when it comes time to read data from those databases (because they were apparently udpated), only the most recent stuff is read.
4. Logs cache. The idea is to keep a cache of the rows needed to build the KV dictionary. Its access is serialized as part of the memory queue, so it can be manipulated and queried to be consistent with the KV dictionary wihtout having to care about the databases. The translation between log rows and the KV dictionary is thus completely done within the memory queue, instead of the current messy interplay between the memory and database queue during sync.
5. Communication between the queues. It's all asynchronous (except for initial loading, see below) and done by passing subsets of log rows, using value semantics (immutable snapshots).

The diagram:

                                                                               
             memory queue                                                      
            ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐                      
                       KV dictionary                                           
            │        ┌─┬────────┬──────────────────┐    │                      
    client           │ │Key1    │Value1            │                           
    get KV◀─┼────────│ ├────────┼──────────────────┤    │                      
               ┌────▶│ │Key2    │Value2            │                           
            │  │     │ ├────────┼──────────────────┤    │                      
               │     │ │Key3    │Value3            │                           
            │  │  ┌─▶└─┴────────┴──────────────────┘    │                      
               │  │                                                            
            │  │  │                                     │                      
    client     │  │    Logs cache (A = local device)                           
    set KV──┼──┤  │    ┌──────┬──────┬──────┬──────┐    │   database queue     
               │  │    │DEVICE│TIME  │KEY   │VALUE │       ┌ ─ ─ ─ ─ ─ ─ ─ ┐   
            │  │  │    └──────┴──────┴──────┴──────┘    │      local db         
               │  │  ┌─┬──────┬──────┬──────┬──────┬─┐     │  ┌──────┐     │   
            │  │  │  │ │A     │      │      │      │ │  │     ├──────┤         
               └────▶│ ├──────┼──────┼──────┼──────┤ │─────┼─▶├──────┤     │   
            │     │  │ │A     │      │      │      │ │  │     └──────┘         
                  │  └─┴──────┴──────┴──────┴──────┴─┘     │               │   
            │     │  ┌─┬──────┬──────┬──────┬──────┬─┐  │      foreign dbs     
                  │  │ │B     │      │      │      │ │     │  ┌──────┐     │   
            │     │  │ ├──────┼──────┼──────┼──────┤ │  │┌────├──────┤         
                  │  │ │B     │      │      │      │ │   │ │  ├──────┤     │   
            │     └──│ ├──────┼──────┼──────┼──────┤ │◀─┼┤    └──────┘         
                     │ │B     │      │      │      │ │   │ │  ┌──────┐     │   
            │        │ ├──────┼──────┼──────┼──────┤ │  ││    ├──────┤         
                     │ │C     │      │      │      │ │   └─┼──├──────┤     │   
            │        └─┴──────┴──────┴──────┴──────┴─┘  │     └──────┘         
             ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   └ ─ ─ ─ ─ ─ ─ ─ ┘   
    
    [Created with Monodraw]

Now for the different events and how they are handled in the above:

1. Initial loading. This is a special step that only needs to happens once, as the very first thing to do, and that requires a synchronous call from the memory queue into the database queue. The database queues reads all the databases and gather all the rows for all the possible key+device configurations (only keeping the most recent timestamps for each), and sends that back to the memory queue, which populates the log cache, then the KV dictionary.
2. Client sets a new KV pair. The memory queue updates the corresponding KV entry in the dictionary. It also updates the row in the cache, and send a copy of that row asynchronously to the database queue for saving.
3. Foreign database gets updated ("sync event"). The database reads all the foreign database up to the previous point (fs events cannot tell which database was changed, so they need to be all read), aggregates the results into a bunch of log rows, and sends a copy asynchronously to the memory queue. The memory queue updates its log cache. Since both queues are serial, updates will always come in the correct order even if there are multiple sync events triggered. So the memory queue can overwrite the corresponding rows if relevant (only keeping one row per device/key combination). From the new cache, we can update the KV dictionary.

Except for the initial loading, the flow for the local database is always in the same direction, and the flow for foreign databases is the other way around:

                                                  
     memory queue                 database queue  
    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ┐
                               │                  
    │ KV dict ──▶ logs cache ────┼──▶ local db   │
                               │                  
    │                            │               │
      KV dict ◀── logs cache ◀─┼───── foreign db  
    │                            │               │
     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ 
    
    [Created with Monodraw]

In the above, we still have the option to manage the database open/close as we like, see point (2) in the outline: either keep everything opened all the time; or more likely, keep the local database open for a little while after the last save (in case more is in the way), and close the foreign databases right after use (since sync events should be rare anyway).