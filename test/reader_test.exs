defmodule FootbalEngine.ReaderTest do
  use ExUnit.Case

  alias FootbalEngine.Reader

  @moduletag :reader

  describe "search" do
    @describetag :search

    test "returns all rows of a given header" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "SP1",
        "Season"    => 201_819,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_3 = %{
        "Div"       => "SP2",
        "Season"    => 201_516,
        "Date"      => ~D[2016-08-20],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Sociedad",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_4 = %{
        "Div"       => "E0",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1, entry_2])
      div_sp2 = MapSet.new([entry_3])
      season_201617 = MapSet.new([entry_4])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Div", "SP2"}, div_sp2},
        {{"Season", "201617"}, season_201617}
      ]

      deps = [
        storage_get_all: fn -> test_data end,
        storage_get: fn(:indexation_status, _default) -> {:ok, :ready} end
      ]

      {:ok, actual}    = Reader.search([{"Div", []}], deps)
      expected  = [entry_1, entry_2, entry_3]

      assert actual == expected
    end

    test "returns error when header does not exist" do
      entry = %{
        "Div"       => "SP2",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP2"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get_all: fn -> test_data end,
        storage_get: fn(:indexation_status, _default) -> {:ok, :ready} end
      ]

      {:error, :invalid_headers, headers} = Reader.search([{"Banana", []}], deps)
      expected_headers  = ["Banana"]

      assert headers == expected_headers
    end

    test "returns error when cache is not ready" do
      entry = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP1"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)  -> {:error, :indexation_not_ready}
          (_key, _default)                -> MapSet.new([entry])
        end,
        storage_get_all: fn -> test_data end
      ]

      assert {:error, :indexation_not_ready} = Reader.search([{"Div", ["SP1"]}], deps)
    end

    test "returns empty when key does not exist" do
      entry = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP1"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)  ->  {:ok, :ready}
          (_key, default)                 ->  default
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = Reader.search([{"Div", ["SP2"]}], deps)
      expected  = []

      assert actual == expected
    end

    test "returns all rows belonging to given key" do
      entry = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      test_data = [
        {{"Div", "SP1"}, MapSet.new([entry])},
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)  -> {:ok, :ready}
          (_key, _default)                -> MapSet.new([entry])
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = Reader.search([{"Div", ["SP1"]}], deps)
      expected  = [entry]

      assert actual == expected
    end

    test "returns all rows belonging to multiple keys" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "E0",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-11],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "La Coruna",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1])
      div_e0 = MapSet.new([entry_2])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Div", "E0"}, div_e0}
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)  -> {:ok, :ready}
          ({"Div", "SP1"}, _default)      -> div_sp1
          ({"Div", "E0"}, _default)       -> div_e0
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = Reader.search([{"Div", ["SP1", "E0"]}], deps)
      expected  = [entry_1, entry_2]

      assert actual == expected
    end

    test "returns rows for keys that exist" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_718,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1])

      test_data = [
        {{"Div", "SP1"}, div_sp1}
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)  -> {:ok, :ready}
          ({"Div", "SP1"}, _default)      -> div_sp1
          ({"Div", _}, _default)          -> MapSet.new()
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = Reader.search([{"Div", ["SP1", "SP99999"]}], deps)
      expected  = [entry_1]

      assert actual == expected
    end

    test "returns commun rows for sets from different headers" do
      entry_1 = %{
        "Div"       => "SP1",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-21],
        "HomeTeam"  => "Ath Madrid",
        "AwayTeam"  => "Alaves",
        "FTHG"      => 1,
        "FTAG"      => 1,
        "FTR"       => "D",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_2 = %{
        "Div"       => "SP1",
        "Season"    => 201_819,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      entry_3 = %{
        "Div"       => "E0",
        "Season"    => 201_617,
        "Date"      => ~D[2016-08-19],
        "HomeTeam"  => "La Coruna",
        "AwayTeam"  => "Eibar",
        "FTHG"      => 2,
        "FTAG"      => 1,
        "FTR"       => "H",
        "HTHG"      => 0,
        "HTAG"      => 0,
        "HTR"       => "D"
      }

      div_sp1 = MapSet.new([entry_1, entry_2])
      season_201617 = MapSet.new([entry_3, entry_1])

      test_data = [
        {{"Div", "SP1"}, div_sp1},
        {{"Season", "201617"}, season_201617}
      ]

      deps = [
        storage_get: fn
          (:indexation_status, _default)    -> {:ok, :ready}
          ({"Div", "SP1"}, _default)        -> div_sp1
          ({"Season", "201617"}, _default)  -> season_201617
        end,
        storage_get_all: fn -> test_data end
      ]

      {:ok, actual}    = Reader.search(
        [
          {"Div",     ["SP1"]},
          {"Season",  ["201617"]}
        ],
        deps
      )
      expected  = [entry_1]

      assert actual == expected
    end
  end

end
