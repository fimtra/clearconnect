## Data Model Concepts {#data-model-concepts}

All data on the platform is represented using _records_. A record is a dictionary (or map) of key-value field pairs. An example of a record with 3 fields might look like this:

| Key | Value |
| --- | --- |
| "Simple message" | "Hello world" |
| "Sides in a square" | 4 |
| "pi" | 3.1415926535898 |

The keys are strings that identify the field. The value of a field can be one of the following _native_ types;

*   `TextValue` (String)
*   `LongValue` (8-byte integral)
*   `DoubleValue` (8-byte floating point)
*   `BlobValue` (byte[])
*   `SubMap`

Records are identified by a name. The name of a record is unique within its declaring _service_ (see [Platform Concepts](platform_concepts.md) for more on services).

### Nesting {#nesting}

A record allows nesting of record structures by using a _sub-map_ to represent the nested record structure. However, unlike a record, a sub-map does not allow further nesting. This means that nesting is limited to a depth of 1; a parent record with many child sub-maps.

A nesting depth of 1 using sub-maps provides a simple mechanism for describing 2 dimensional data structures. This is sufficient, for example, to represent the data in a database table.

### Atomic Changes {#atomic-changes}

An _atomic change_ to a record defines a group of fields and their values that change in a single transactional action. Observers of records only receive atomic changes. The scope of an atomic change is defined by the publisher.

Atomic changes provide consistency for record changes, a natural delta (incremental change) mechanism for sending record changes and a means to control data publishing rates.

#### Example {#example}

Consider the following record with this initial field state:

| Key | Value |
| --- | --- |
| "Field 1" | 0 |
| "Field 2" | 0 |
| "Field 3" | 0 |

The following sequence of independent changes occurs to the record in the publisher;

1.   Field 1 value set to 1
1.   Field 2 removed
1.   Field 1 value set to 2
1.   Field 4 added with value 5

When the publisher publishes the atomic change to this record, all the independent changes above are combined into a single atomic change and distributed to observers. Observers of the record see the record state change, atomically, from the initial state above to this below:

| Key | Value |
| --- | --- |
| "Field 1" | 2 |
| "Field 3" | 0 |
| "Field 4" | 5 |

A publisher can, of course, publish an atomic change for every single field change to a record. However, this approach can be inefficient for rapidly changing data sets and causes unnecessary network traffic.

### Image-on-subscribe {#image-on-subscribe}

The concept of ‘image-on-subscribe’, in the context of real-time data systems, describes the pattern where a subscriber for data receives a current image of the data when it subscribes and after that receives only the deltas (incremental changes) of the initial image. The key principle of image-on-join semantics is that the subscriber must start consuming deltas from the point-in-time that the image represents, otherwise the state of the data item is incoherent.

In the ClearConnect platform, all data model subscription follows the image-on-subscribe pattern. The atomic changes to the records are the deltas. Records include a publish sequence number that is used to ensure received atomic changes are in the expected sequence. An out of sequence atomic change results in a re-sync operation for the record.