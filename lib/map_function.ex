defmodule MapReduce.MapFunction do
  def map(line, partition_pid) do
    send(partition_pid, {:map_key_put, self()})
    Enum.each(String.split(line, " "), fn word -> send(partition_pid, {:map_value_put, word}) end)
  end
end
