defmodule FootbalEngine.Populator.Parser do
  @moduledoc """
  Contains several parsing utility functions to parse the data from the CSV to
  the Elixir native types.
  """

  @spec parse_data(map) :: {:ok, map} | {:error, atom, String.t}
  def parse_data(entry) do
    with  {:ok, season} <-  parse_integer(Map.get(entry, "Season")),
          {:ok, fthg}   <-  parse_integer(Map.get(entry, "FTHG")),
          {:ok, ftag}   <-  parse_integer(Map.get(entry, "FTAG")),
          {:ok, hthg}   <-  parse_integer(Map.get(entry, "HTHG")),
          {:ok, htag}   <-  parse_integer(Map.get(entry, "HTAG")),
          {:ok, date}   <-  parse_date(Map.get(entry, "Date"))
    do
      parsed_data =
        entry
        |> Map.delete("")
        |> Map.put("Season", season)
        |> Map.put("FTHG", fthg)
        |> Map.put("FTAG", ftag)
        |> Map.put("HTHG", hthg)
        |> Map.put("HTAG", htag)
        |> Map.put("Date", date)

      {:ok, parsed_data}
    end
  end

  @spec parse_integer(String.t) ::
    {:ok, integer}
    | {:error, :unable_to_parse_int, String.t}
  def parse_integer(val) do
    case Integer.parse(val) do
      {num, _remainder} -> {:ok, num}
      _err              -> {:error, :unable_to_parse_int, val}
    end
  end

  @spec parse_date(String.t) :: {:ok, Date.t} | {:error, atom, String.t}
  def parse_date((<< day::bytes-2, "/", month::bytes-2, "/", year::bytes-4>>) = date_str) do
    with  {:ok, year}   <- parse_integer(year),
          {:ok, month}  <- parse_integer(month),
          {:ok, day}    <- parse_integer(day),
          {:ok, date}   <- Date.new(year, month, day)
    do
      {:ok, date}
    else
      {:error, :invalid_date} -> {:error, :invalid_date, date_str}
      err -> err
    end
  end

  def parse_date(bad_date), do: {:error, :date_with_bad_format, bad_date}

end
