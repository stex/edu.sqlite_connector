require_relative '../sqlite_connector'

describe '#use_database' do
  let(:database_file) { File.join(File.dirname(__FILE__), 'test.sqlite3') }

  after(:each) do
    File.unlink(database_file)
  end

  it "returns the yielded block's return value" do
    result = use_database('test') { 'something' }
    expect(result).to eql 'something'
  end
end

describe SQLiteWrapper::Connector do

  describe '#execute' do
    before(:all) do
      use_database('test') do |db|
        db.execute('CREATE TABLE users(id INTEGER, name TEXT);')
        db.execute('CREATE TABLE posts(id INTEGER, content TEXT);')
        db.execute('INSERT INTO users (id, name) VALUES (1, "Charlie Brown");')
        db.execute('INSERT INTO users (id, name) VALUES (2, "Snoopy");')
        db.execute('INSERT INTO posts (id, content) VALUES (1, "Worf Worf");')
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