module Histogram
  extend self

  ##
  # Get an array with counts of records for each range of values.
  # The +table+ argument is the name of the database table to query and
  # +value+ is an SQL statement for the value to calculate for each record.
  #
  def bands(table, expr, options = {})
    options[:min]        ||= minimum_value(table, expr)
    options[:max]        ||= maximum_value(table, expr)
    options[:bands]      ||= 500
    options[:conditions] ||= nil
    inc = (options[:max].to_f - options[:min].to_f) / options[:bands].to_f
    res = ActiveRecord::Base.connection.execute(
      "SELECT band, COUNT(*) AS count FROM (" +
        "SELECT FLOOR(" +
          "(#{expr} - #{options[:min].to_f}) / #{inc}" +
        ") AS band FROM #{table}" +
        ((c = options[:conditions]) ? " WHERE #{c}" : "") +
      ") bands " +
      "GROUP BY band ORDER BY band"
    )
    ([0] * options[:bands]).tap do |bands|
      case adapter = ActiveRecord::Base.connection.adapter_name
      when "PostgreSQL"
        res.each{ |i| bands[i["band"].to_i] = i["count"].to_i }
      when "Mysql2"
        res.each{ |i| bands[i[0]] = i[1] }
      else
        raise StandardError, "Database adapter '#{adapter}' not supported"
      end
    end
  end

  ##
  # Get the minimum value from the table.
  #
  def minimum_value(table, expr)
    min_or_max_value(table, expr, false)
  end

  ##
  # Get the maximum value from the table.
  #
  def maximum_value(table, expr)
    min_or_max_value(table, expr, true)
  end


  private # -----------------------------------------------------------------

  ##
  # Get the min or max value from the table.
  #
  def min_or_max_value(table, expr, max = false)
    res = ActiveRecord::Base.connection.execute(
      "SELECT #{max ? 'MAX' : 'MIN'}(#{expr}) FROM #{table}"
    )
    case adapter = ActiveRecord::Base.connection.adapter_name
    when "PostgreSQL"
      res.first.first[1].to_f
    when "Mysql2"
      res.first[0]
    else
      raise StandardError, "Database adapter '#{adapter}' not supported"
    end
  end
end
