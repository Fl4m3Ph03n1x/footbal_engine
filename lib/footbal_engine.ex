defmodule FootbalEngine do
  @moduledoc """
  Database engine. It can read the CSV file and index its contents for faster
  query searches. It can also query the indexed data.

  This module simulates an indexed (basic) memory db, where all the data is
  placed into memory and replicated for faster query searches. Memory usage will
  be inversionally proportional to query search times, meaning the more memory
  this solution uses, the faster the response times.

  It also uses :persistent_term tables, from OTP 21 becasue we are expected to
  perform a ton of reads, but only to write to the table once (at startup).

  More info on :persistent_term:
  http://erlang.org/doc/man/persistent_term.html
  """

  alias FootbalEngine.{Populator, Reader}

  @doc """
  Reads the given CSV file and indexes it into memory. If such is not possible,
  it keeps trying to read the file and populate it until the operation is
  completely successful.

  Example:
  ```
  path = "./Data.csv"
  {:ok, _pid} = FootbalEngine.new(path)
  ```

  Arguments:

  - `path :: String.t` - the path of the file to read.

  Returns:

  - `{:ok, pid}` the pid of the GenServer to be supervised.
  """
  defdelegate new(path), to: Populator

  @doc """
  Queries the indexed tables for information.

  Example:
  ```
  path = "./Data.csv"
  {:ok, :indexation_successful} = FootbalEngine.new(path)
  {:ok, results} = FootbalEngine.search(
    [{"Div", ["SP1", "SP2"]}, {"Season", ["201617"]}]
  )
  ```

  Arguments:

  - `query :: [{String.t, [String.t]}]` - The query to perform. `query` is a
  list of tuples with the format `{header :: String.t, [values :: String.t]}`.
  For example, if I want to search for all the games in Div SP1 or E0, the
  following tuple would cover them `{"Div", ["SP1", "E0"]}`. If and I all games
  in Div SP1 and with HomeTeam Barcelona, the following list would cover this
  `[{"Div", ["SP1"]}, {"HomeTeam", ["Barcelona"]}]`. You can also send the values
  array empty to get all the games: `[{"Div", []]` would get all games with that
  have a Div.

  Returns:

  - `{:ok, [Map.t]}` if the search was successfull. The second element of the
  tuple is a list with all the entries matching the given result in a Map
  format.

  - `{:error, :invalid_headers, [String.t]}` if some of the headers given were
  invalid, meaning they are missing from the CSV. The third element of the tuple
  is the list of the invalid headers.

  - `{:error, :no_valid_data_to_save}` if the CSV file was empty, or had no
  valid data to index. This can happen if the all rows in the file were
  malformed or generated some kind of error.

  - `{:error, any}` if the CSV file could not be indexed at all due to an error.
  The second element of the tuple is the reason of the error.

  - `{:error, :engine_not_started}` if `FootbalEngine.new(String.t)` was not
  called beforehand.
  """
  defdelegate search(query), to: Reader

  @doc """

  """
  defdelegate child_spec(args), to: FootbalEngine.Populator.Server
end
