#! /usr/bin/env ruby

require 'mysql2'
require 'simple-spreadsheet'
require 'roo'
require 'csv'

@db_host = 'localhost'
@db_user = 'root'
@db_pass = ''
@db_name = 'apulian'
@db_port = '8889'

excel_file = 'Apulian_Database.xls'
csv_file   = 'Apulian_Database.csv'
@create_table = '';
@build_table = '';

@excel = ''

def read_file(filename)
  if filename =~ /xlsx$/
    @excel = Roo::Excelx.new(filename)
  else
    @excel = Roo::Excel.new(filename)
  end
end

def convert_excel(filename, output)
  read_file(filename)
  output = File.open(output, "w")

  1.upto(@excel.last_row) do |line|
    output.write CSV.generate_line @excel.row(line)
  end
end

puts "Connecting to Database"
client = Mysql2::Client.new(
  :host => @db_host,
  :username => @db_user,
  :password => @db_pass,
  :port => @db_port
)

puts "Setting up database"
client.query("DROP DATABASE IF EXISTS #{@db_name};")
client.query("CREATE DATABASE #{@db_name};")
client.query("USE \`#{@db_name}\`;")

# convert the Excel file to a CSV file, because Excel is a crazy format to work
# with. Seriously, it's bonkers...
puts "Converting #{excel_file} to #{csv_file}"
convert_excel(excel_file, csv_file)

puts "Creating table script"
csv = CSV.read(csv_file, :headers => true)

client.query("DROP TABLE IF EXISTS #{@db_name}")
@create_table = "CREATE TABLE #{@db_name} ("
@create_table += '`id` int(10) unsigned NOT NULL,'
csv.headers.each do |header|
  @create_table += "\`#{header}\` VARCHAR(255) NULL,"
end

@create_table += ' UNIQUE KEY  `id_UNIQUE` (`id`)'
@create_table += ') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;'

# TODO there are multiple fields with the name names (e.g. Instrument(s) & #)
puts @create_table

# loop over each row and create a hash of values
CSV.foreach(csv_file, :headers => true) do |row|
  values = row.to_hash
  @build_table = "INSERT INTO #{@db_name} (#{values.keys} VALUES ("
    values.each do |index, value|
      @build_table += "'#{value}', "
      # @build_table += ", " unless values.to_a.last?
    end
  @build_table += ");"
  puts @build_table;
  # puts row
end


client.close # release the connection
