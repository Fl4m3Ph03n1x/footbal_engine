defmodule FootbalEngine.Populator.Storage do
  @moduledoc """
  Stores the given data in memory, indexing it as necessary along the way.

  Can return a complete success (if all the data was valid and inserted into
  memory), a partial success (if some data was faulty) or and error (depending
  on the reason it failed).

  No data is also treated as an error case because an empty memory DB is
  useless and is therefore likely to be result of a human error.
  """

  @spec persist({[map], [any]}, keyword) ::
    {:ok, :indexation_successful}
    | {:ok, :partial_indexation_successful, [any]}
    | {:error, :no_valid_data_to_save, [any]}
  def persist({valid_data, invalid_data}, deps), do:
    valid_data
    |> store_data(deps)
    |> store_status(invalid_data)

  @spec store_data(list, keyword) ::
    {:ok, :save_successful}
    | {:error, :no_valid_data_to_save}
  defp store_data([], _deps), do: {:error, :no_valid_data_to_save}

  defp store_data(parsed_data, deps) do
    headers =
      parsed_data
      |> hd
      |> Map.keys()

    parsed_data
    |> generate_all_keys(headers)
    |> Enum.each(fn {key, val} -> deps[:storage_put].(key, val) end)

    {:ok, :save_successful}
  end

  @spec generate_all_keys([map], [String.t]) ::
    [{{String.t, String.t}, MapSet.t}]
  defp generate_all_keys(data, headers), do:
    Enum.flat_map(headers, &generate_header_keys(data, &1))

  @spec generate_header_keys([map], String.t) ::
    [{{String.t, String.t}, MapSet.t}]
  defp generate_header_keys(data, header) do
    data
    |> Stream.map(&get_header_values(&1, header))
    |> Stream.uniq()
    |> Enum.map(&create_index(&1, header, data))
  end

  @spec get_header_values(map, String.t) :: String.t
  defp get_header_values(entry, header), do: Map.get(entry, header)

  @spec create_index(String.t, String.t, [map]) ::
    {{String.t, String.t}, MapSet.t}
  defp create_index(curr_val, header, data) do
    matches =
      data
      |> Enum.filter(fn entry -> Map.get(entry, header) == curr_val end)
      |> MapSet.new()

    key = {header, to_string(curr_val)}
    {key, matches}
  end

  @spec store_status({:ok, atom}, [any]) ::
    {:ok, :indexation_successful}
    | {:ok, :partial_indexation_successful, [any]}
    | {:error, atom, [any]}
  defp store_status({:ok, _save_msg}, [] = _bad_data), do:
    {:ok, :indexation_successful}

  defp store_status({:ok, _save_msg}, bad_data), do:
    {:ok, :partial_indexation_successful, bad_data}

  defp store_status({_save_status,  save_msg}, bad_data), do:
    {:error, save_msg, bad_data}

end
