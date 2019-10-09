defmodule FootbalEngine.MixProject do
  use Mix.Project

  def project do
    [
      app: :footbal_engine,
      version: "2.0.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:runtime_tools]
    ]
  end

  defp deps do
    [
      { :csv, "~> 2.3"  },

      # tracing
      { :observer_cli, "~> 1.5" },

      # tests and dev
      { :dialyxir,        "~> 1.0.0-rc.6",  only: [:dev],         runtime: false  },
      { :credo,           "~> 1.0.0",       only: [:dev, :test],  runtime: false  },
      { :excoveralls,     "~> 0.10",        only: [:test],        runtime: false  },
      { :mix_test_watch,  "~> 0.8",         only: [:dev],         runtime: false  },
      { :ex_doc,          "~> 0.19",        only: [:dev],         runtime: false  }
    ]
  end
end
