require 'rubygems'
require 'hpricot'
require 'open-uri'
require 'matrix'
require 'activesupport'

class ScheduleParser
  attr_reader :doc, :table, :map, :schedule
  attr_accessor :is_day, :is_time
  
  def initialize(url, klass, options = {})
    @is_day = /Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|[0-9]?[0-9]\/[0-9][0-9]/
    @is_time = /([0-9]?[0-9]\:[0-9][0-9])|([0-9][AP]M)/
    
    @day_row_index = options[:day_row_index]
    @time_column_index = options[:time_column_index]
    @filter = (options[:filter].nil? ? true : options[:filter])
    
    open(url) do |source|
      @doc = Hpricot(source)
      @table = (@doc/klass)
      @map = produce_array(@table) 
    end
  end

  def parse
    @schedule = parse_data(@map)    
    @schedule = combine_days(@schedule)
    # @schedule = change_times_to_ranges(@schedule)
    @schedule
  end
  
  def find_day_row(table_map = @map)
    rowindex, day_row = nil
        
    if @day_row_index
      day_row_index = @day_row_index
      day_row = table_map[@day_row_index]
    else
      table_map.each_with_index do |row, index| 
        day_row = nil
        hit_count = 0
        day_row_index = 0
        row.each do |cell|
          if !cell.nil? && cell =~ @is_day && cell.length < 30
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
    
    return {:index => day_row_index, :data => format_day_row(day_row)}
  end
  
  def format_day_row(row)
    row.each_with_index do |cell, index|       
      row[index] = cell.sub(/([y])([0-9])/, "#{$1} #{$2}")
    end
    row
  end
  
  def clean_cell_content(td)  
    if @filter
      content = td.inner_text
      content.gsub(/\r?\n?\s+/, " ").strip
    else
      td.inner_html
    end
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
      end
    end
  end

  def find_time_column(table_map = @map)
    matrix = Matrix.rows(table_map)
    time_column = nil, time_column_index = 0
    
    if @time_column_index
      time_column_index = @time_column_index      
      time_column =  matrix.column_vectors[@time_column_index]      
    else
      matrix.column_vectors.each_with_index do |column, colindex|
        hit_count = 0
        column.to_a.each_with_index do |cell, rowindex|
          if !cell.nil? && cell  =~ @is_time
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
    
    return {:index => time_column_index, :data => format_time_column(time_column)}
  end

  def produce_array(table = @table)
    trs = table/"tr"
    map = Array.new(100) { Array.new(7) }
    trs.each_with_index do |tr, rowindex|
      tds = tr/"td"
      tds.each_with_index do |td, colindex|

        row_span_count = td.attributes['rowspan'] || 1
        col_span_count = td.attributes['colspan'] || 1

        td_value = clean_cell_content(td)

        # find the next available cell
        until map[rowindex][colindex].nil?
          colindex = colindex + 1
        end

        # copy the entry to other cells and rows to fulfil colspan and rowspan
        col_span_count.to_i.times do |col|
          row_span_count.to_i.times do |row|
            map[rowindex + row][colindex + col] = td_value
          end
        end
      end
    end
    map
  end

  def parse_data(table_map = @map)
    schedule = {}
    days = find_day_row(table_map)    
    times = find_time_column(table_map)

    table_map.each_with_index do |row, rowindex|
      if (rowindex > days[:index])
        row.each_with_index do |cell, colindex|
          if (colindex > times[:index]) && !cell.nil?
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
end


puts "KUT"
kut = ScheduleParser.new("http://kut.org/about/schedule", "div['rendered_page_item']/table")
puts kut.parse.inspect
puts ""

puts "KMSU"
kmsu = ScheduleParser.new("http://www.mnsu.edu/kmsufm/schedule/", "div.msu-content-one-col-container/table")
puts kmsu.parse.inspect
puts ""

puts "KNAU"
knau = ScheduleParser.new("http://www.publicbroadcasting.net/knau/guide.guidemain", "//table[@class='grid']", {:time_column_index => 0, :day_row_index => 1})
puts knau.parse.inspect
puts ""

