# Resource message

Resource is FieldLink message ID 2. It carries broad Atlas resource operations
without defining a separate FieldLink message for every API endpoint.

The complete message is UTF-8 JSON. FieldLink validates the operation envelope
and that each body is representable as JSON. The receiving Atlas application
validates the Entity, Object, or Task data against the authoritative Atlas API
schema before using it.

The two-radio console can opt the destination adapter into an Atlas Resource
gateway. That adapter loads the Atlas SDK, executes one request received from
the preflight-approved source radio, and sends the SDK result back as a
Resource response.

## Request contract

Every request starts with these fields:

```json
{
  "type": "resource",
  "kind": "request",
  "operation": "get",
  "request_id": "req-123",
  "resource_type": "task"
}
```

`request_id` correlates exactly one response with its request. It is an
application identifier, not a FieldLink transfer ID or delivery receipt.

| Operation | Resources                  | Additional fields           | Meaning                                               |
| --------- | -------------------------- | --------------------------- | ----------------------------------------------------- |
| `create`  | `entity`, `object`         | `body`                      | Create from Atlas JSON. Object JSON is metadata only. |
| `get`     | `entity`, `object`, `task` | `resource_id`               | Read one resource.                                    |
| `list`    | `entity`, `object`, `task` | `query: { limit, cursor? }` | Read a bounded page of resources.                     |
| `patch`   | `entity`, `object`         | `resource_id`, `body`       | Apply an Atlas JSON patch.                            |
| `delete`  | `entity`, `object`         | `resource_id`               | Delete one resource.                                  |

`list.query.limit` must be from 1 through 1000. A list response normalizes the
selected Atlas resource page to `items`, `has_more`, and optional
`next_cursor`; it does not forward the unrelated Entity, Task, and Object pages
returned by the SDK's full-dataset query.

Create example:

```json
{
  "type": "resource",
  "kind": "request",
  "operation": "create",
  "request_id": "req-create-entity",
  "resource_type": "entity",
  "body": {
    "entity_id": "rescue-1",
    "entity_type": "vehicle"
  }
}
```

List example:

```json
{
  "type": "resource",
  "kind": "request",
  "operation": "list",
  "request_id": "req-list-tasks",
  "resource_type": "task",
  "query": {
    "limit": 50,
    "cursor": "next-page"
  }
}
```

## Response contract

Every operation uses one response shape:

```json
{
  "type": "resource",
  "kind": "response",
  "request_id": "req-123",
  "status": 200,
  "body": {
    "id": "task-123",
    "status": "assigned"
  }
}
```

`status` is the numeric Atlas API result. `body` is optional because a success
or error may have no JSON payload. Receiving a response proves neither RF
delivery nor application success beyond the result it reports.

## Boundaries

Resource is a typed operation envelope, not a generic HTTP tunnel. It carries
no method, URL, route, header, API key, or arbitrary query parameter. Receiving
the message on a normal FieldLink node does not execute it. Atlas API execution
is enabled only on the destination adapter for a real Resource request test.

The following stay outside Resource:

- Task create, assignment, acknowledgement, progress, completion, failure, and
  cancellation. Task lifecycle needs explicit Task semantics.
- Object upload and download bytes. Resource carries Object metadata only.
- Entity check-in and other domain actions that are not CRUD.
- Unsolicited Task push. The push contract remains a separate Task message
  decision.

The test gateway accepts requests only from radio A's preflight Node ID. It
caches 64 request IDs for the adapter process lifetime: repeating identical
JSON replays the first response, while using the same ID for different JSON
returns `409` without calling Atlas. This protects an accidental retry but is
not strong sender authentication. Any channel member can spoof a FieldLink
Node ID, so this API-key-backed gateway is for dedicated test radios on a
trusted MeshCore channel, not an authorization boundary for deployment.

Atlas resources can contain Atlas-owned metadata such as a resource version.
That JSON remains an Atlas domain fact; the Resource envelope adds no FieldLink
version or revision field.

## Atlas SDK test gateway

Until the Atlas SDK is published, the destination adapter imports the built SDK
from an Atlas Modernization checkout. Build that checkout first:

```bash
cd "/absolute/path/to/Atlas Modernization"
npm ci
npm run build:sdk
```

Copy `.env.example` to `.env` in Atlas-FieldLink and fill in:

```dotenv
ATLAS_MODERNIZATION_PATH=/absolute/path/to/Atlas Modernization
ATLAS_BASE_URL=https://your-atlas-api.example
ATLAS_API_KEY=your-api-key
```

`.env` is ignored by Git. Only the destination adapter loads it when the
controller enables the gateway. The key never enters the FieldLink Resource
JSON, adapter control messages, or evidence artifacts.

Use `--resource-request` to send a JSON file through the real two-radio path:

```bash
npm run fieldlink -- test \
  --a /dev/cu.usbmodem-A \
  --b /dev/cu.usbmodem-B \
  --message resource \
  --resource-request request.json \
  --retry-strategy selective-window \
  --allow-inbox-drain
```

The CLI validates the file as one Resource request before it opens either
radio. After radio preflight, adapter B handshakes with Atlas through the SDK,
then A sends the request. The run passes only after A receives the matching
Resource response, any fragmented response transfer finishes, cleanup
succeeds, and neither adapter reports a listener or protocol error. The final
summary keeps request and response transfer statistics separate and reports the
Atlas response status independently from FieldLink correlation and digest
verification. It distinguishes a clean delivery from one recovered by fragment
repair or a repeated receipt request.

The encoded Resource message shares FieldLink's 1 MiB limit. Messages over 132
encoded bytes use the normal FieldLink transfer protocol.
