# Running CCH with Docker

```
git submodule update --init --recursive
docker build -t routing_engine .
docker run -p <target port>:80 --mount 'type=bind,src=<folder containing here csvs>,dst=/import' --mount 'type=volume,src=routing_engine_data,dst=/data' routing_engine
```

Here import will take 10 - 20 minutes and might need a lot of RAM for the link geometry.
FlowCutter some hours.
Actual CCH preprocessing around 10 minutes

## API

*Non of this should be considered final*

There are currently three API endpoints:

`GET /query` takes 4 parameters:

* `from_lat`: `float`
* `from_lng`: `float`
* `to_lat`: `float`
* `to_lat`: `float`

These points will be used to find a start and end node using a nearest neighbor search.

The endpoint returns a json response of the following form:

```json
{
  "distance": 42,
  "path": [[42.23, 23.42], [43.24, 24.43]]
}
```

`"distance"` contains the total travel time in ms.
`"path"` an array of pairs with lat lng pairs.
If no path exists the response will be empty (very bad API design here... 🙈).

When used while preprocessing (or customization) is still running, this endpoint will block and wait until it can execute the query.
Might lead to browser timeouts.

`GET /here_query` takes 6 parameters:

* `from_link_id`: `int`
* `from_direction`: `bool`
* `from_link_fraction`: `float`
* `to_link_id`: `int`
* `to_direction`: `bool`
* `to_link_fraction`: `float`

The link ids have to exist within the given here map.
If not the query will return a HTTP 500.
Again probably not the best API design in the moment.
The direction parameter indicates if the link is to be taken in `FromRef` direction (`true`) or `FromRef` (`false`).
Finally the fraction indicates where on the link the query should start.
For the future this should probably be made more flexible to catch the case where both directions are fine.

```json
{
  "distance": 42,
  "path": [[42, true], [45, false], [32, true]]
}
```

`"distance"` contains the total travel time in ms.
`"path"` an array of here link ids and directions.
If no path exists the response will be empty.

When used while preprocessing (or customization) is still running, this endpoint will block and wait until it can execute the query.
Might lead to browser timeouts.

`POST /customize` takes its parameters as json.

The input has to be an array of pairs.
Each pair is an array of exactly three values.
The first one is the here link id.
The second one is a boolean indicating if the weight is to be applied in `FromRef` direction (`true`) or `ToRef` (`false`).
The third one is the new travel time in ms.
The weight has to be an integer smaller than 2^31-1 or `null` (to set the weight to infinity).
Currently the new value will be applied to the link in both directions.
If a link id does not exist, the pair will be ignored.
The new values will be carried over into future customizations.

This endpoint will immediatly return an empty response.
The customization will happen in the background.
Currently, new queries will block until the customization is done.
