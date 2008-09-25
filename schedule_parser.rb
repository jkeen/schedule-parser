require 'rubygems'
require 'hpricot'
require 'open-uri'
require 'matrix'
require 'activesupport'

class TableMap
  attr_reader :map
  def initialize(url, selector, options={})
    open(url) do |source|
      @options=options.reverse_merge!({:filter => true, :convert_spans => true})      
      @doc = Hpricot(source)
      @table = (@doc/selector)
    end
    process(@table)
    yield self if block_given?
  end
    
  def process(table = @table)
    trs = table/"tr"    
    map = Array.new(trs.size) { Array.new(max_row_width(trs)) }
    trs.each_with_index do |tr, rowindex|
      (tr/"td").each_with_index do |td, colindex|
        td_value = clean_cell_content(td)

        # find the next available cell
        until map[rowindex][colindex].nil?
          colindex = colindex + 1
        end

        # copy the entry to other cells and rows to fulfil colspan and rowspan
        if @options[:convert_spans]
          row_span_count = td.attributes['rowspan'] || 1
          col_span_count = td.attributes['colspan'] || 1
    
          col_span_count.to_i.times do |col|
            row_span_count.to_i.times do |row|
              map[rowindex + row][colindex + col] = td_value
            end
          end
        else
           map[rowindex][colindex] = td_value
        end
      end
    end
    @map = map
  end
  
  def minor(xrange, yrange)
    matrix = Matrix.rows(@map)
    matrix.minor(xrange, yrange).to_a
  end
  
  private
  
  def max_row_width(trs)
    trs.collect { |tr| (tr/"td").size }.max
  end
  
  def clean_cell_content(content)
    if @options[:filter]
      if @options[:cell_filter]
        @options[:cell_filter].call(content)
      else
        content.inner_text.gsub(/\r?\n?\s+/, " ").strip      
      end
    else
      content.inner_html
    end
  end
end


class ScheduleParser
  attr_reader :doc, :table, :schedule
  attr_accessor :map
  
  IS_DAY = /Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|[0-9]?[0-9]\/[0-9][0-9]/
  IS_TIME = /([0-9]?[0-9]\:[0-9][0-9])|([0-9][AP]M)/
  
  class << self
    def from_map(map, options = {})
      parser = new({:map => map}, options)
      yield parser if block_given?
    end
    
    def from_url(url, selector, options = {})
      parser = new({:url => url, :selector => selector}, options)
      yield parser if block_given?
    end
  end
  
  def initialize(params = {}, options = {})
   if params.is_a?(Hash)
      @options = options
      if params[:map]
        @map = params[:map]
      else
        @map = TableMap.new(params[:url], params[:selector], options).map
      end
    else
       raise "Use ScheduleParser.from_map(table_map, options), and ScheduleParser.from_url(url, selector, options) constructors for initialization"
    end
  end
  
  def process
    @schedule = parse_data(@map)    
    @schedule = combine_days(@schedule)
    # @schedule = change_times_to_ranges(@schedule)
    @schedule
  end
  
  def find_day_row(table_map = @map)    
    day_row_index, day_row = nil
    if @options[:day_row].is_a? Integer
      day_row_index = @options[:day_row]
      day_row = table_map[@options[:day_row]]
    elsif @options[:day_row].is_a? Array
      day_row_index = 0
      day_row =  @options[:day_row]
    else
      table_map.each_with_index do |row, index| 
        day_row = nil
        hit_count, day_row_index = 0
        row.each do |cell|
          if !cell.nil? && cell =~ IS_DAY && cell.length < 30
            hit_count = hit_count + 1
            if (hit_count > 2)
              day_row = row
              day_row_index = index
              break
            end
          end
        end    
        unless (day_row.nil?)
          break
        end
      end
    end
    
    raise 'Could not find the row of days in the table' if day_row.nil?
    return {:index => day_row_index, :data => format_day_row(day_row)}
  end
  
  def find_time_column(table_map = @map)
    matrix = Matrix.rows(table_map)
    time_column = nil, time_column_index = 0
        
    if @options[:time_column].is_a? Integer
      time_column_index = @options[:time_column]      
      time_column =  matrix.column_vectors[@options[:time_column]]
    elsif @options[:time_column].is_a? Array
      time_column_index = 0
      time_column =  @options[:time_column]
    else
      matrix.column_vectors.each_with_index do |column, colindex|
        hit_count = 0
        column.to_a.each_with_index do |cell, rowindex|
          if !cell.nil? && cell  =~ IS_TIME
            hit_count = hit_count + 1
          end
        end
        if hit_count > 4
          time_column = column
          time_column_index = colindex
          break
        end
      end
    end
    raise 'Could not find the column of times in the table' if time_column.nil?
    return {:index => time_column_index, :data => format_time_column(time_column)}
  end
  
  def format_day_row(row)
    row.each_with_index do |cell, index|
      row[index] = cell.gsub(/(day)([0-9])/, "#{$1} #{$2}")
    end
    row
  end

  def format_time_column(column)
    meridiem = nil
    column = column.to_a
    column.each_with_index do |cell, index|
      if (cell =~ /midnight/i)
        cell = "12:00"
        column[index] = "12:00 AM"
        meridiem = "AM"
      elsif (cell =~ /noon/i)
        cell = "12:00"
        column[index] = "12:00 PM"
        meridiem = "PM"
      end      
      unless cell =~ /[AP]M/
        column[index] = "#{cell} #{meridiem}" unless cell.nil?
        column[index].strip!
      end
    end
  end

  def parse_data(table_map = @map)
    schedule = {}
    days = find_day_row(table_map)    
    times = find_time_column(table_map)

    table_map.each_with_index do |row, rowindex|
      if (rowindex > days[:index] || day_row_supplied?)
        row.each_with_index do |cell, colindex|
          if (colindex > times[:index] || time_column_supplied?) && !cell.nil?
            (schedule[cell] ||= [])   <<  {:day => days[:data][colindex], :time => times[:data][rowindex]}
          end
        end
      end
    end
    schedule
  end

  def combine_days(schedule)
    schedule.each do |name, data|
      days = {}
      data.each do |sched|
        (days[sched[:day]] ||= []) << sched[:time]
      end
      schedule[name] = days
    end
    schedule
  end

  def change_times_to_ranges(schedule)
    schedule.each do |name, data|
      data.each do |day, times|
        times = times.collect { |time| DateTime.parse(time) }
        times = times.sort
        data[day] = {:start_time => times.first, :end_time => times.last + 30.minutes}        
      end
    end
    schedule
  end
  
  private
  
  def day_row_supplied?
    @options[:day_row].is_a? Array
  end

  def time_column_supplied?
    @options[:time_column].is_a? Array
  end
end


=begin
# Examples of usage 

ScheduleParser.from_url("http://kut.org/about/schedule", "div['rendered_page_item']/table") do |schedule|
  puts "KUT"
  puts schedule.process.inspect
end

ScheduleParser.from_url("http://www.mnsu.edu/kmsufm/schedule/", "div.msu-content-one-col-container/table") do |schedule|
  puts "KMSU"
  puts schedule.process.inspect
end
 
ScheduleParser.from_url("http://www.publicbroadcasting.net/knau/guide.guidemain", "//table[@class='grid']", {:time_column => 0, :day_row => 1}) do |schedule|
  puts "KNAU"
  puts schedule.process.inspect
end
 
ScheduleParser.from_url("http://www.publicbroadcasting.net/wuwm/guide.guidemain?t=1", "//table[@class='grid']", {:time_column => 0, :day_row => 1}) do |schedule|
  puts "WUWM"
  puts schedule.parse.inspect
end

TableMap.new("http://minnesota.publicradio.org/radio/services/the_current/schedule/index.php?day=mon", "//table", {:cell_filter => lambda{ |content| content.inner_text.gsub(/\r?\n?\s+/, " ").strip }}) do |table|
  # chop off the uneeded fields
  map = table.minor(2..(table.map.size - 3),0..1)
  ScheduleParser.from_map(map, {:time_column => 0, :day_row => ["", "Monday"]}) do |schedule|
    schedule.process.each { |s| puts s.inspect}
  end
end

=end
