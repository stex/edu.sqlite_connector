#
# A short example to demonstrate the 2D array return values
#

require_relative('../sqlite_connector')

drop_database('arrays') {}
use_database('arrays') do |db|
  db.execute('CREATE TABLE users(id INTEGER, name TEXT);')
  db.execute('INSERT INTO users (id, name) VALUES (1, "Charlie Brown");')
  db.execute('INSERT INTO users (id, name) VALUES (2, "Snoopy");')

  puts 'Values are returned as 2D array without column names:'
  puts db.execute('SELECT * FROM users;').inspect
end

