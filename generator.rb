require 'json'

STDOUT.sync = true

i = 1

def event(message, i)
	{eventId: i, message: "#{message} #{i}"}
end

while true do
	puts event(ARGV[0], i).to_json
	STDERR.puts "WROTE #{i}"
	i = i + 1
	sleep(1)
end