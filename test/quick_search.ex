defmodule QuickSearchTest do
  use ExUnit.Case

  doctest QuickSearch

  alias FootbalEngine.QuickSearch

  setup do
    :ets
  end

  describe "division" do
      test "returns tuples for all divisions" do
        actual    = QuickSearch.division(:all)
        expected  = [
          {
            :sp1,
            [
              {:sp1,  201617, ~D[2016-08-19],	"La Coruna",  "Eibar",    2,  1,  "H",	0,	0,	"D"},
              {:sp1,	201617,	~D[2016-08-19],	"Malaga",     "Osasuna",  1,	1,  "D",  0,	0,	"D"}
            ]
          },
          {
            :e0,
            [
              {:e0,  201617,  ~D[2016-11-06], "Arsenal",  "Tottenham",  1,  1,  "D",	1,	0,  "H"}
            ]
          },
          {
            :d1,
            [
              {:d1,	201617, ~D[2016-12-17],   "Mainz",    "Hamburg",    3,  1,	"H",	1,	1,	"D"}
            ]
          }
        ]

        assert actual == expected
      end

      test "returns empty when divisions is empty" do
        actual    = QuickSearch.division([:dv2])
        expected  = []

        assert actual == expected
      end

      test "returns all tuples belonging to 1 division" do
        actual    = QuickSearch.division([:dv1])
        expected  = [
          {
            :sp1,
            [
              {:sp1,  201617, ~D[2016-08-19],	"La Coruna",  "Eibar",    2,  1,  "H",	0,	0,	"D"},
              {:sp1,	201617	~D[2016-08-19],	"Malaga"    ,	"Osasuna",  1,	1,  "D",  0,	0,	"D"}
            ]
          }
        ]

        assert actual == expected
      end

      test "returns all tuples belonging to 2 more divisions" do
        actual    = QuickSearch.division([:dv1, :e0])
        expected  = [
          {
            :sp1,
            [
              {:sp1,  201617, ~D[2016-08-19],	"La Coruna",  "Eibar",    2,  1,  "H",	0,	0,	"D"},
              {:sp1,	201617	~D[2016-08-19],	"Malaga"    ,	"Osasuna",  1,	1,  "D",  0,	0,	"D"}
            ]
          },
          {
            :e0,
            [
              {:e0,  201617,	~D[2016-11-06],	"Arsenal"	"Tottenham",	1,	1,	"D",	1,	0,	"H"}
            ]
          }
        ]

        assert actual == expected
      end
  end

end
