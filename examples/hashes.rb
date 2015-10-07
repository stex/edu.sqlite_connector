#
# A short example to demonstrate the hash return values
#

require_relative('../sqlite_connector')

drop_database('hashes') {}
use_database('hashes', :return_format => 'hash') do |db|
  db.execute('CREATE TABLE users(id INTEGER, name TEXT);')
  db.execute('INSERT INTO users (id, name) VALUES (1, "Charlie Brown");')
  db.execute('INSERT INTO users (id, name) VALUES (2, "Snoopy");')

  puts 'Values are returned as array of hashes:'
  puts db.execute('SELECT * FROM users;').inspect
end
