require_relative '../sqlite_connector'

describe SQLiteWrapper::Connector do

  let(:database_file) { File.join(File.dirname(__FILE__), 'test.sqlite3') }

  before(:each) do
    use_database('test') do |db|
      db.execute('CREATE TABLE users(id INTEGER, name TEXT);')
      db.execute('CREATE TABLE posts(id INTEGER, content TEXT);')
      db.execute('INSERT INTO users (id, name) VALUES (1, "Charlie Brown");')
      db.execute('INSERT INTO users (id, name) VALUES (2, "Snoopy");')
      db.execute('INSERT INTO posts (id, content) VALUES (1, "Worf Worf");')
    end
  end

  after(:each) do
    File.unlink(database_file)
  end

  describe '#use_database' do
    it "returns the yielded block's return value" do
      result = use_database('test') { 'something' }
      expect(result).to eql 'something'
    end
  end

  describe '#execute' do
    context 'when given a select query' do
      context 'with table aliases' do
        let(:query) { 'select u.name from users u;' }

        it 'does not thrown an exception' do
          use_database('test', :return_format => 'hash') do |db|
            expect { db.execute(query) }.not_to raise_exception
          end
        end

        it 'names the resulting columns correctly' do
          use_database('test', :return_format => 'hash') do |db|
            expect(db.execute(query)).to all include('name')
          end
        end
      end
    end

    context 'when inserting a multiline text' do
      let(:insert_query) { "INSERT INTO users (id, name) VALUES(3, 'I\nam\nGroot!');" }
      let(:select_query) { 'SELECT * FROM users WHERE id = 3;' }

      it 'actually inserts the newlines' do
        use_database('test', :return_format => 'hash') do |db|
          db.execute(insert_query)
          expect(db.execute(select_query)).to have(1).item
          expect(db.execute(select_query).first).to include('name' => "I\nam\nGroot!")
        end
      end
    end

    context 'when inserting a text with multiple spaces' do
      let(:insert_query) { "INSERT INTO users (id, name) VALUES(3, 'I   am    Groot!');" }
      let(:select_query) { 'SELECT * FROM users WHERE id = 3;' }

      it 'actually inserts the spaces' do
        use_database('test', :return_format => 'hash') do |db|
          db.execute(insert_query)
          expect(db.execute(select_query)).to have(1).item
          expect(db.execute(select_query).first).to include('name' => 'I   am    Groot!')
        end
      end
    end

    context 'when selecting all columns from multiple tables' do
      context 'using a single "*"' do
        let(:query) { 'SELECT * FROM users, posts;' }

        context 'and hash results' do
          context 'with ambiguous column names' do
            it 'raises an exception' do
              use_database('test', :return_format => 'hash') do |db|
                expect { db.execute(query) }.to raise_error SQLiteWrapper::AmbiguousColumnNameError
              end
            end
          end
        end

        context 'and array results' do
          it 'returns the full amount of columns per row' do
            use_database('test') do |db|
              expect(db.execute(query)).to all have_exactly(4).items
            end
          end
        end
      end

      context 'using "table.*"' do
        let(:query) { 'SELECT users.*, posts.* FROM users, posts;' }

        context 'and array results' do
          it 'returns the full amount of columns per row' do
            use_database('test') do |db|
              expect(db.execute(query).map(&:size)).to all eql 4
            end
          end
        end
      end
    end
  end
end
