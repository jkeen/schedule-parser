require 'rubygems'
require 'hpricot'
require 'open-uri'
require 'matrix'
require 'activesupport'

#handling day overflows.  KMSU and KNAU have some interesting problems

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
  
  ## CONSTRUCTORS  ###########################################################################################
  
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
    @map = table_map = (options[:map] ? options[:map] : TableMap.new(options).map)
    
    puts table_map.inspect
    
    @data = {}
    @data[:map] = table_map
    @data[:day_row] = find_day_row(table_map)
    @data[:time_column] = find_time_column(table_map)
    
    @data[:day_row].merge!({:formatted => format_day_row(@data[:day_row][:data])})
    @data[:time_column].merge!({:formatted => format_time_column(@data[:time_column][:data])})
    
    
    yield self if block_given?
  end
    
  def process
    @schedule = parse_data(@map)
    
    @schedule = convert_days_and_times_into_dates(@schedule)
    @schedule = condense_ranges(@schedule)
  end
  
  def parse_data(table_map = @map)
    schedule = {}
    days = @data[:day_row]
    times = @data[:time_column]

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
    
  ## FIND COLUMNS #################################################################################################
  def find_day_row(table_map = @map)    
    
    # use user-specified day row if supplied
    return {:index => 0, :data => @options[:day_row]} if @options[:day_row]    

    # use user-specified row index if supplied
    return {:index => @options[:day_row_index], :data => table_map[@options[:day_row_index]]} if @options[:day_row_index]
    
    # find the day row and return it along with the index
    table_map.each_with_index do |row, index| 
      if row.to_a.map { |r| is_day?(r) }.select { |r| r }.size > 2
        return {:index => index, :data => row}
        break
      end
    end
    
    raise 'Could not find the row of days in the table'
  end
  
  def find_time_column(table_map = @map)
    matrix = Matrix.rows(table_map)
    
    # use user-specified time column if supplied
    return {:index => 0, :data => options[:time_column]} if @options[:time_column]

    # use user-specified time column index if supplied
    return {:index => @options[:time_column_index], :data => matrix.column_vectors[@options[:time_column_index]]} if @options[:time_column_index]

    # find the time column and return it along with the index    
    matrix.column_vectors.each_with_index do |column, colindex|
      if column.to_a.map { |c| is_time?(c) }.select { |c| c }.size > 4
        return {:index => colindex, :data => column.to_a}
        break
      end
    end
    
    raise 'Could not find the column of times in the table'
  end
  

  ## FORMAT ROWS/COLUMNS ###########################################################################################

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
    day_offset = 0
    @first_time_index=nil, @first_time = nil
    cells.each_with_index do |cell, index|
      results = cell.scan(/(([0-9]{1,2})(:[0-9]{1,2})?\s?([AaPp][Mm])?)/)
      if results.many?
        # time in cell is formatted like: 6:30 - 7:30pm
        range = absolutize_time_pair({:start_time => results.first.first, :end_time => results.last.first})
      elsif results.size == 1
        # time in cell is single, like 6:00pm.  It's end time is the value in the cell below.
        # we can't take look head for it yet, because we don't know if it's a range or a single yet
        range = absolutize_time_pair({:start_time => cell, :end_time => :next})
      else
        range = {}      
      end
      
      # this if the first time we've seen a time
      if !@first_time && range[:start_time] 
        @first_time = DateTime.parse(range[:start_time]) 
        @first_time_index = index
      end
      ranges << range
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
      
      times[:start_day_offset] = ((DateTime.parse(times[:start_time]) <= @first_time && index > @first_time_index) ? 1 : 0) if times[:start_time]
      times[:end_day_offset] = ((DateTime.parse(times[:end_time]) <= @first_time && index > @first_time_index) ? 1 : 0) if times[:end_time]
    end
    ranges
  end
  
  def absolutize_time_pair(times)
    # make sure am and pm are on both the start and end time
    times[:start_time] = "#{times[:start_time]}#{times[:end_time].scan(/[AaPp][Mm]/).first}" unless times[:start_time] =~ /[AaPp][Mm]/ unless times[:start_time] == :next
    times[:end_time] = "#{times[:end_time]}#{times[:start_time].scan(/[AaPp][Mm]/).first}" unless times[:end_time] =~ /[AaPp][Mm]/ unless times[:end_time] == :next 
    
    return times
  end
  
  def format_date_for_output(date)
    date.strftime("%A %I:%M%p")
  end

  ## DATE PROCESSING ###########################################################################################
  def convert_days_and_times_into_dates(schedule)  
    schedule.each do |name, data|
      times = [] 
      data.each do |sched|
        combined = {}
        [sched[:times]].flatten.each do |time|
          # handle overflows by adding (day offset).days        
          combined[:start_time] =  DateTime.parse("#{sched[:day]} #{time[:start_time]}") + (time[:start_day_offset]).day
          combined[:end_time] = DateTime.parse("#{sched[:day]} #{time[:end_time]}") + (time[:end_day_offset]).day
        end
        times << combined
      end
      schedule[name] = times
    end
    schedule
  end

  def condense_ranges(schedule)
    # converts Monday 7:30pm -> Monday 8:00pm, Monday 8:00pm -> Monday 8:30pm into Monday 7:30pm -> Monday 8:30pm
    
    schedule.each do |show_name, raw_times|
      raw_times = raw_times.sort_by { |t| t[:start_time] }
      until raw_times.empty? do
        selected_time_pair = raw_times.first if (!selected_time_pair)
        
        # this is a valid range, add it
        (time_range ||= []) << selected_time_pair[:start_time]
        time_range << selected_time_pair[:end_time]
        
        # look for a link between this end time, and another pair's start time
        match = raw_times.find { |matching_pair| matching_pair[:start_time] == selected_time_pair[:end_time] }
        if !match
          # match wasn't found, add the current time range to the final ranges, and look for another range
          (all_ranges_for_day ||= []) << time_range
          time_range = []    
        end
        # delete the pairs that we've already looked at
        raw_times.delete_if { |matching_pair| matching_pair[:start_time] == selected_time_pair[:start_time] && matching_pair[:end_time] == selected_time_pair[:end_time]}
        selected_time_pair = match # set time to the match if matched, and to nil if not matched              
      end

      simplified_ranges_for_day = []
      all_ranges_for_day.each do |range|
        simplified_ranges_for_day  << {:start_time => format_date_for_output(range.first), :end_time => format_date_for_output(range.last)}
      end
      schedule[show_name] = simplified_ranges_for_day 
    end
    schedule
  end
  
  private
  def is_day?(text)
    text =~ /Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|[0-9]?[0-9]\/[0-9][0-9]/ && text.length < 30
  end
  
  def is_time?(text)
    text =~ /([0-9]?[0-9]\:[0-9][0-9])|([0-9][AaPp][Mm])/
  end
  
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
# # 
ScheduleParser.from_url("http://kut.org/about/schedule", "div.rendered_page_item/table") do |schedule|
  puts "KUT"
  results = schedule.process
  puts results.to_yaml
end

ScheduleParser.from_url("http://www.mnsu.edu/kmsufm/schedule/", "div.msu-content-one-col-container/table") do |schedule|
  puts "KMSU"
  puts schedule.process.to_yaml
end

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
# TableMap.new({:url => "http://minnesota.publicradio.org/radio/services/the_current/schedule/index.php?day=mon", :selector => "div.document/table", :cell_filter => lambda{ |content| content.inner_text.gsub(/\r?\n?\s+/, " ").strip }}) do |table|
#   # chop off the uneeded fields
#   map = table.minor(1..(table.map.size - 2),0..1)
#   
#   ScheduleParser.from_map(map, {:time_column_index => 0, :day_row => ["", "Monday"]}) do |schedule|
#     puts schedule.process.inspect
#   end
# end
# 
# 