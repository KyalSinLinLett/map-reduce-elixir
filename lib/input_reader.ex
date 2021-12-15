defmodule MapReduce.InputReader do
  alias MapReduce.MapFunction, as: Map
  def read(file_name, partition_pid) do
    case File.read(file_name) do
      {:ok, file_body} ->
        Enum.each(Regex.split(~r/\r|\n|\r\n/, String.trim(file_body)), fn line -> spawn(fn -> Map.map(line, partition_pid) end) end)
      {:error, reason} ->
        IO.puts :stderr, "FILE READ ERROR WITH REASON #{inspect reason}"
    end
  end
end
