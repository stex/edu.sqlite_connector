#
# A simple interactive console to execute SQL commands
#

require_relative('../sqlite_connector')

use_database("veranst") do |db|
  puts("Willkommen in der interaktiven SQLite-Konsole")
  puts("Ende mit .quit")
  puts()
  command = ""
  while command != ".quit" do
    print("sqlite> ")
    command = gets.chop
    begin
      if command != ".quit" then
        table = db.execute(command)
        for i in 0..table.size-1 do
          for j in 0..table[i].size-2 do
            print(table[i][j].to_s+"|");
          end
          puts(table[i][table[i].size-1].to_s)
        end
        puts table.size.to_s + " Zeilen zurückgegeben."
      end
    rescue Exception => e
      puts "Ungültige Eingabe: " + e.message.red
    end
  end
end
