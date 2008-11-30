require 'rubygems'
require 'hpricot'
require 'open-uri'
require 'matrix'
require 'activesupport'

class TableMap
  attr_reader :map
  def initialize(options={})
    open(options[:url]) do |source|
      #convert_spans => convert column and row spans into single cells, i.e. td colspan=4 => td td td td
      #filter => cleans up cell content, stripping new lines, etc
      
      @options=options.reverse_merge!({:filter => true, :convert_spans => true})      
      @doc = Hpricot(source)
      @table = (@doc/options[:selector])
    end
    @map = process(@table)
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
  
  def self.from_map(map, options = {})
    parser = new({:map => map}.merge(options))
    yield parser if block_given?
  end
  
  def self.from_url(url, selector, options = {})
    parser = new({:url => url, :selector => selector}.merge(options))
    yield parser if block_given?
  end
  
  def initialize(options = {})
    raise "Use ScheduleParser.from_map(table_map, options), and ScheduleParser.from_url(url, selector, options) constructors for initialization" unless options.is_a?(Hash)
    
    # options:
    # :url => url of schedule
    # :selector => selector of table
    
    # :day_row => (optional) specify the row index of the table where the days are located instead of searching for it
    # :time_row => (optional) specify the column index where the times are located instead of searching for it 
    
    @options = options
    @map = (options[:map] ? options[:map] : TableMap.new(options).map)
    yield self if block_given?
  end
  
  def detect_format(table_map = @map)
    @format ||= {}
    @format[:days] = find_day_row(table_map)     
    @format[:times] = find_time_column(table_map)
    @format[:time_format] = detect_time_format(@format[:times])
  end
  
  def detect_time_format(times)
    format = times[:data].to_a.select { |t| is_time?(t) }.map { |t| !(t =~ /[0-9]+\s?([AaPp][Mm])?\s?-\s?[0-9]+\s?([AaPp][Mm])?/).nil? }
    case format.uniq.size
      when 2
        return :mixed
      when 1
        return :ranges if format.first
        return :singles unless format.first
    end
  end
  
  
  def is_day?(text)
    text =~ /Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|[0-9]?[0-9]\/[0-9][0-9]/ && text.length < 30
  end
  
  def is_time?(text)
    text =~ /([0-9]?[0-9]\:[0-9][0-9])|([0-9][AaPp][Mm])/
  end
  
  def process
    detect_format(@map)
    format_key_columns
        
    @schedule = parse_data(@map)   
    
    @schedule = combine_days(@schedule)
    # @schedule = change_times_to_ranges(@schedule)
    # puts @schedule.to_yaml
  end
    
  def find_day_row(table_map = @map)    
    return {:index => 0, :data => @options[:day_row]} if @options[:day_row]    
    return {:index => @options[:day_row_index], :data => table_map[@options[:day_row_index]]} if @options[:day_row_index]
    
    table_map.each_with_index do |row, index| 
      if row.to_a.map { |r| is_day?(r) }.select { |r| r }.size > 2
        return {:index => index, :data => row}
        break
      end
    end
    
    raise 'Could not find the row of days in the table' if day_row.nil?
  end
  
  def find_time_column(table_map = @map)
    matrix = Matrix.rows(table_map)

    return {:index => 0, :data => options[:time_column]} if @options[:time_column]
    return {:index => @options[:time_column_index], :data => matrix.column_vectors[@options[:time_column_index]]} if @options[:time_column_index]
    
    matrix.column_vectors.each_with_index do |column, colindex|
      if column.to_a.map { |c| is_time?(c) }.select { |c| c }.size > 4
        return {:index => colindex, :data => column.to_a}
        break
      end
    end
    
    raise 'Could not find the column of times in the table' if time_column.nil?
  end
  
  def format_key_columns
    @format[:days].merge!({:formatted => format_day_row(@format[:days][:data])})
    @format[:times].merge!({:formatted => format_time_column(@format[:times][:data])})
  end
  
  def format_day_row(row)
    cells = row.map do |cell|
      cell.gsub(/(day)([0-9])/, "#{$1} #{$2}")
    end
    cells
  end

  def format_time_column(column)
    # replace all time names with numbers
    cells = column.to_a.map { |c| c.gsub(/noon/i, "12:00pm").gsub(/midnight/i, "12:00am") }
    
    # make sure all times have AM or PM attached
    cells = cells.collect do |cell|
      if (is_time?(cell))
        am_or_pm = cell.scan(/[AaPp][Mm]/).first
        @last_am_or_pm = am_or_pm unless am_or_pm.nil?
        cell = "#{cell}#{@last_am_or_pm}" unless cell.match(/[AaPp][Mm]/)
      end
      cell
    end
    
    split_times_into_start_and_end(cells)
  end
  
  def split_times_into_start_and_end(cells)
    #split ranges into start and end
    ranges = []
    cells.each_with_index do |cell, index|
      results = cell.scan(/(([0-9]{1,2})(:[0-9]{1,2})?\s?([AaPp][Mm])?)/)
      if results.many?
        ranges << absolutize_time_pair({:start_time => results.first.first, :end_time => results.last.first})
      elsif results.size == 1
        ranges << absolutize_time_pair({:start_time => cell, :end_time => :next})
      else
        ranges << {}      
      end
    end
    
    #find end time's marked next, and replace them with next start time
    ranges.each_with_index do |times, index|
      if times[:end_time] == :next 
        if ranges.size == index + 1
          times[:end_time] = ranges.select { |r| !r.nil? && r[:start_time] }.first[:start_time]
        else
          times[:end_time]=ranges[index + 1][:start_time]        
        end
      end      
    end
    ranges
  end
  
  def absolutize_time_pair(times)
    times[:start_time] = "#{times[:start_time]}#{times[:end_time].scan(/[AaPp][Mm]/).first}" unless times[:start_time] =~ /[AaPp][Mm]/ unless times[:start_time] == :next
    times[:end_time] = "#{times[:end_time]}#{times[:start_time].scan(/[AaPp][Mm]/).first}" unless times[:end_time] =~ /[AaPp][Mm]/ unless times[:end_time] == :next 
    
    return times
  end
  
  def parse_data(table_map = @map)
    schedule = {}
    days = @format[:days]
    times = @format[:times]

    table_map.each_with_index do |row, rowindex|
      if (rowindex > days[:index] || day_row_supplied?)
        row.each_with_index do |cell, colindex|
          if (colindex > times[:index] || time_column_supplied?) && !cell.nil?
            (schedule[cell] ||= [])   <<  {:day => days[:formatted][colindex], :times => times[:formatted][rowindex]}
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
        (days[sched[:day]] ||= []) << sched[:times]
      end
      schedule[name] = days
    end
    schedule
  end

  def change_times_to_ranges(schedule)
    # puts schedule.inspect
    schedule.each do |show_name, data|
      data.each do |day, raw_times|
        raw_times = raw_times.sort_by { |t| DateTime.parse(t[:start_time]) }
        until raw_times.empty? do
          selected_time_pair = raw_times.first if (!selected_time_pair)
          
          # this is a valid range, add it
          time_range ||= [] << selected_time_pair[:start_time]
          time_range << selected_time_pair[:end_time]
          
          # look for a link between this end time, and another pair's start time
          match = raw_times.find { |matching_pair| matching_pair[:start_time] == selected_time_pair[:end_time] }
          if match
            # add the found start time to the range 
            time_range << match[:start_time]
          else
            # match wasn't found, add the current time range to the final ranges, and look for another range
            all_ranges_for_day ||= [] << time_range
            time_range = []    
          end
          # delete the pairs that we've already looked at
          raw_times.delete_if { |matching_pair| matching_pair[:start_time] == selected_time_pair[:start_time] && matching_pair[:end_time] == selected_time_pair[:end_time]}
          selected_time_pair = match # set time to the match if matched, and to nil if not matched
        end
        
        simplified_ranges_for_day = []
        all_ranges_for_day.each do |range|
          simplified_ranges_for_day  << {:start_time => DateTime.parse(range.first).strftime("at %I:%M%p"), :end_time => DateTime.parse(range.last).strftime("at %I:%M%p")}
        end
        
        data[day] = simplified_ranges_for_day      
      end
    end
    schedule
  end
  
  private
  
  def day_row_supplied?
    @options[:day_row]
  end

  def time_column_supplied?
    @options[:time_column]
  end
end


class KUTParser < ScheduleParser
  def initialize
    super({:url => "http://kut.org/about/schedule", :selector => "div['rendered_page_item']/table"})
  end
end

# 
# KUTParser.new() do |schedule|
#   puts "KUT"
#   puts schedule.process.inspect
# end


# Examples of usage 

# ScheduleParser.new({:url => "http://www.kruiradio.org/schedule/", :selector => "table.MsoNormalTable"}) do |schedule|
#   puts "KRUI"
#   puts schedule.process.to_yaml #.inspect
# end
# 
# ScheduleParser.from_url("http://kut.org/about/schedule", "div.rendered_page_item/table") do |schedule|
#   puts "KUT"
#   puts schedule.process.to_yaml
# end
# 
# ScheduleParser.from_url("http://www.mnsu.edu/kmsufm/schedule/", "div.msu-content-one-col-container/table") do |schedule|
#   puts "KMSU"
#   puts schedule.process.to_yaml
# end
# 
# ScheduleParser.from_url("http://www.publicbroadcasting.net/knau/guide.guidemain", "//table[@class='grid']", {:time_column_index => 0, :day_row_indexh => 1}) do |schedule|
#   puts "KNAU"
#   puts schedule.process.to_yaml
# end
# #  
# ScheduleParser.from_url("http://www.publicbroadcasting.net/wuwm/guide.guidemain?t=1", "//table[@class='grid']", {:time_column_index => 0, :day_row_index => 1}) do |schedule|
#   puts "WUWM"
#   puts schedule.parse.to_yaml
# end
# 
TableMap.new({:url => "http://minnesota.publicradio.org/radio/services/the_current/schedule/index.php?day=mon", :selector => "div.document/table", :cell_filter => lambda{ |content| content.inner_text.gsub(/\r?\n?\s+/, " ").strip }}) do |table|
  # chop off the uneeded fields
  map = table.minor(1..(table.map.size - 2),0..1)
  
  ScheduleParser.from_map(map, {:time_column_index => 0, :day_row => ["", "Monday"]}) do |schedule|
    puts schedule.process.inspect
  end
end
# 
# 
