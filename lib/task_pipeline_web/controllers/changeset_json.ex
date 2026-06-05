defmodule TaskPipelineWeb.ChangesetJSON do
  @moduledoc """
  Translates Ecto Changeset error validation lists into semantic JSON map payloads.
  """

  @doc """
  Traverses errors in a changeset and flattens them into a clean key-value dictionary map.
  Suppressed from exploding when Ecto includes multi-layered meta tuples (e.g. parameterized enums).
  """
  def error(%{changeset: changeset}) do
    %{
      errors:
        Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
          Enum.reduce(opts, msg, fn {key, value}, acc ->
            # Only attempt interpolation if the parameter value is a simple type.
            # If it is a tuple/map (like parameterized enum types), bypass safely.
            if safe_stringifiable?(value) do
              String.replace(acc, "%{#{key}}", to_string(value))
            else
              acc
            end
          end)
        end)
    }
  end

  # Allow standard types that safely implement String.Chars protocol
  defp safe_stringifiable?(val)
       when is_atom(val) or is_binary(val) or is_integer(val) or is_float(val),
       do: true

  defp safe_stringifiable?(_other), do: false
end
