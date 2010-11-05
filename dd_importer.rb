require 'yaml'
require 'faster_csv'
require 'ftools'

class DdImporter
  ### NOTE: the field numbers for custom fields will be different
  ### in your version of PowerSchool.  You can look them up using
  ### the 'Fields' or 'FieldsTable' field.   The ID number of
  ### the record corresponding to the field Name is what you want.
  
  STUDENTS_HEADERS = %w{
    id
    student_number
    state_studentnumber
    schoolid
    first_name
    last_name
    dob
    fedethnicity
    gender
    enroll_status
    grade_level
    mother_first
    mother
    father_first
    father
    street
    city
    state
    zip
    home_phone
    schoolentrydate
    districtentrydate
    entrydate
    exitdate
    [39]alternate_school_number
    ca_parented
    ca_homelanguage
    ca_elastatus
    ca_daterfep
    ca_firstusaschooling
  }

  TEACHERS_HEADERS = %w{
    id
    teachernumber
    schoolid
    [39]alternate_school_number
    first_name
    last_name
    email_addr
    status
    staffstatus
  }

  COURSES_HEADERS = %w{
    course_number
    course_name
    credit_hours
    credittype
    ca_courselevel
    schoolid
    [39]alternate_school_number
  }

  STUDENT_SCHEDULES_HEADERS = %w{
    studentid
    teacherid
    schoolid
    termid
    [01]state_studentnumber
    [05]teachernumber
    [39]alternate_school_number
    [01]grade_level
    expression
    abbreviation
    course_number
    section_number
    sectionid
  }

  FLUENCY_CODES = {
    'EO' => 1,
    'IFEP' => 2,
    'EL' => 3,
    'RFEP' => 4,
    'TBD' => 5
  }

  TERM_ABBRS = {
    '01-02' => 'YR',
    '02-03' => 'YR',
    '03-04' => 'YR',
    '04-05' => 'YR',
    '05-06' => 'YR',
    '06-07' => 'YR',
    '07-08' => 'YR',
    '08-09' => 'YR',
    '09-10' => 'YR',
    '10-11' => 'YR',
    '11-12' => 'YR',
    'HT 1' => 'H1',
    'HT 2' => 'H2',
    'HT 3' => 'H3',
    'HT 4' => 'H4',
    'HT 5' => 'H5',
    'HT 6' => 'H6',
    'HT1' => 'H1',
    'HT2' => 'H2',
    'HT3' => 'H3',
    'HT4' => 'H4',
    'HT5' => 'H5',
    'HT6' => 'H6',
  }
  
  # don't forget to update this every year!
  VALID_YEARS = 
    [ '01-02', '02-03', '03-04', '04-05', '05-06',
      '06-07', '07-08', '08-09', '09-10', '10-11',
      '11-12', '12-13' ]
      
  EXCLUDED_COURSES = [ 
    'AAAA', 'oooo' # Attendance
  ] 
  
  NON_EXCLUDED_COURSES = [
    '0500', '1500', '2500', '3500', '4500', # Bacich Library
    '0820', '1820', '2820', '3820', '4820', # Bacich Art
    '0830', '1830', '2830', '3830', '4830', # Bacich Tech
    '0880', '1880', '2880', '3880', '4880', # Bacich Music
    '0881', '1881', '2881', '3881', '4881', # Bacich Chorus
    '0700', '1700', '2700', '3700', '4700', # Bacich PE
  ]
  
  def self.clean_date(raw_date)
    if raw_date
      date = raw_date.gsub(/-/, "/").strip
      mo = nil
      da = nil
      yr = nil
      if date.match(/^(\d+)\/(\d+)\/(\d+)(\s|$)/)
        mo = $1.to_i
        da = $2.to_i
        yr = $3.to_i
      elsif date.match(/^(\d+)\/(\d+)(\s|$)/)
        mo = $1.to_i
        da = 1
        yr = $2.to_i
      end
      if mo && da && yr
        if yr < 20
          yr += 2000
        elsif yr < 100
          yr += 1900
        end
        if mo >= 1 && mo <= 12 && da >= 1 && da <= 31 && yr > 1900 && yr < 2020
          return sprintf("%02d/%02d/%04d", mo, da, yr)
        end
      end
    end
    nil
  end
  
  def self.name
    "DataDirector Export"
  end
  
  OPTIONS = { 'output_base_dir' => '.', 'year' => '10-11' }
  
  def options
    OPTIONS
  end
  
  def at(progress, total, message)
    puts "at #{progress}/#{total}: #{message}"
  end
  
  def perform
    @rosters = { }
    @users = { }
    @courses = { }
    @students = { }
    @enrollments = { }
    @teacher_years = { }
    @custom_fields = { }
    
    output_base_dir = options['output_base_dir'] or raise "No output_base_dir specified"
    
    @zip_name = "Kentfield-#{Time.now.strftime("%Y%m%d")}"
    @data_dir = File.expand_path(output_base_dir)
    @input_dir = File.join(@data_dir, 'psexport')
    @output_dir = File.join(@data_dir, @zip_name)
    
    @single_year = options['year']
    @single_year = nil if @single_year && !VALID_YEARS.include?(@single_year)
    
    @working_at = 0
    @total_work_units = 10 * (@single_year ? 11 : 9 + 2*VALID_YEARS.size)
    tick_message("Starting job")
    process_files
  end
  
  def zip_file_path
    "#{@output_dir}.zip"
  end
  
  def tick_message(message, incr=0)
    @working_at += incr if incr > 0
    at(@working_at, @total_work_units, message)
  end
  
  def process_csv(path, headers, &block)
    FasterCSV.open(path, :col_sep => "\t", :row_sep => "\n", :headers => headers) do |csv|
      csv.header_convert { |h| h.downcase.tr(" ", "_").delete("^a-z_").to_sym }
      csv.each(&block)
    end
  end

  def analyze_student_data(year)
    num_rows = 0
    process_csv("#{@input_dir}/dd-students.txt", STUDENTS_HEADERS) do |row|
      studentid = row[:id]
      
      parent_name = "#{row[:mother_first]} #{row[:mother]}".strip
      parent_name = "#{row[:father_first]} #{row[:father]}".strip if parent_name.empty?    

      set_student(studentid, :ssid,       row[:state_studentnumber])
      set_student(studentid, :student_id, row[:student_number])
      set_student(studentid, :first_name, row[:first_name])
      set_student(studentid, :last_name,  row[:last_name])
      set_student(studentid, :gender,     row[:gender])
      set_student(studentid, :parent,     parent_name)
      set_student(studentid, :street,     row[:street])
      set_student(studentid, :city,       row[:city])
      set_student(studentid, :state,      row[:state])
      set_student(studentid, :zip,        row[:zip])
      set_student(studentid, :phone_number,          row[:home_phone])
      set_student(studentid, :parent_education,      row[:ca_parented])
      set_student(studentid, :birthdate,             DdImporter.clean_date(row[:dob]))
      set_student(studentid, :date_entered_school,   DdImporter.clean_date(row[:schoolentrydate]))
      set_student(studentid, :date_entered_district, DdImporter.clean_date(row[:districtentrydate]))
      set_student(studentid, :first_us_entry_date,   DdImporter.clean_date(row[:ca_firstusaschooling]))
      set_student(studentid, :date_rfep,             DdImporter.clean_date(row[:ca_daterfep]))

      hispanic_ethnicity = (row[:fedethnicity].to_i == 1) ? '500' : nil
      set_student(studentid, :ethnicity,             hispanic_ethnicity)

      home_language = row[:ca_homelanguage]
      home_language = home_language.to_i unless home_language.nil?
      set_student(studentid, :primary_language,      home_language.to_s)
      
      fluency = row[:ca_elastatus]
      fluency = FLUENCY_CODES.fetch(fluency.upcase, nil) unless fluency.nil?
      set_student(studentid, :language_fluency,      fluency)
      
      set_student(studentid, :gate,       'N')
      set_student(studentid, :nslp,       'N')
      set_student(studentid, :migrant_ed, 'N')
      set_student(studentid, :special_program, 'N')
      set_student(studentid, :title_1,    'N')

      enroll_status = row[:enroll_status].to_i
      enroll_year = date_to_year_abbr(row[:entrydate])
      if enroll_status == 0 || (year == enroll_year && enroll_status > 0)
        set_enrollment(year, studentid, :school_id,   row[:schoolid])
        set_enrollment(year, studentid, :school_code, row[:alternate_school_number])
        set_enrollment(year, studentid, :grade_level, row[:grade_level])
      else
        # puts "skipping enrollment for student #{studentid} entrydate #{row[:entrydate]} for year #{year}, enroll_year #{enroll_year}, enroll_status #{enroll_status}"
      end
      num_rows += 1
      tick_message("#{num_rows} student records analyzed") if num_rows % 100 == 0
    end
  end
  
  def analyze_race_data
    # we bail after we get the first race...
    process_csv("#{@input_dir}/dd-races.txt", true) do |row|
      studentid = row[:studentid]
      next unless @students.has_key?(studentid)
      race = row[:racecd]
      set_student(studentid, :ethnicity, race) unless student(studentid, :ethnicity)
    end
  end
  
  def analyze_program_data
    process_csv("#{@input_dir}/dd-programs.txt", true) do |row|
      studentid = row[:foreignkey]
      next unless @students.has_key?(studentid)
      
      program_code = (row[:user_defined_text] || 0).to_i
      case program_code
      when 122 # Title 1
        set_student(studentid, :title_1,    'Y')
      when 127 # GATE
        set_student(studentid, :gate,       'Y')
      when 135 # Migrant
        set_student(studentid, :migrant_ed, 'Y')
      when 144 # Special Ed
        disability = nil
        # custom has these chars: [17, 4, 3, 18, 0, 3, 50, 52, 48]
        m = (row[:custom] || '').match(/([0-9]{3})$/)
        disability = m[1] if m
        set_student(studentid, :special_program,   'Y')
        set_student(studentid, :primary_disability, disability)
      when 175 # NSLP
        set_student(studentid, :nslp,       'Y')
      end
    end
  end
  
  def analyze_user_data(year)
    num_rows = 0
    process_csv("#{@input_dir}/dd-teachers.txt", TEACHERS_HEADERS) do |row|
      userid = row[:id]
      teacherid = row[:teachernumber]
      
      set_user(userid,  :employee_id,   teacherid)
      set_user(userid,  :teacher_id,    teacherid)
      set_user(userid,  :school_id,     row[:schoolid])
      set_user(userid,  :school_code,   row[:alternate_school_number])
      set_user(userid,  :first_name,    row[:first_name])
      set_user(userid,  :last_name,     row[:last_name])
      set_user(userid,  :email_address, row[:email_addr])
      
      # current teachers or specified administrators
      if row[:status].to_i == 1 && 
        (row[:datadirector_access].to_i == 1 || row[:staffstatus].to_i == 1)
        set_teacher_year(year, userid, :active, 'y')
      end
      
      num_rows += 1
      tick_message("#{num_rows} teacher records analyzed") if num_rows % 100 == 0
    end
  end
  
  def analyze_course_data
    num_rows = 0
    ['dd-courses-bacich.txt', 'dd-courses-kent.txt'].each do |fname|
      process_csv("#{@input_dir}/#{fname}", COURSES_HEADERS) do |row|
        courseid = row[:course_number]
        abbreviation = course_abbreviation(row[:course_name])
        set_course(courseid, :course_id,    courseid)
        set_course(courseid, :abbreviation, abbreviation)
        set_course(courseid, :name,         row[:course_name])
        set_course(courseid, :credits,      row[:credit_hours])
        set_course(courseid, :subject_code, row[:credittype])
        set_course(courseid, :a_to_g,       '')
        set_course(courseid, :school_id,    row[:schoolid])
        set_course(courseid, :school_code,  row[:alternate_school_number])
      
        num_rows += 1
        tick_message("#{num_rows} courses analyzed") if num_rows % 100 == 0
      end
    end
  end
  
  def analyze_roster_data
    num_rows = 0
    ['dd-rosters-bacich.txt', 'dd-rosters-kent.txt'].each do |fname|
      process_csv("#{@input_dir}/#{fname}", STUDENT_SCHEDULES_HEADERS) do |row|
        courseid  = row[:course_number]
        next if EXCLUDED_COURSES.include?(courseid)
      
        studentid = row[:studentid]
        userid    = row[:teacherid]
        sectionid = row[:sectionid]
        next if sectionid.nil?
        sectionid.gsub!(/^[-]/, '')
        period = expression_to_period(row[:expression])
        next if period.nil?
        term   = term_abbreviation(row[:abbreviation])
        next if term.nil?
        memberid = "#{courseid}-#{studentid}"
        year = term_to_year_abbr(row[:termid].gsub(/^[-]/, ''))
      
        set_teacher_year(year, userid, :active, 'y')
      
        set_roster(year, memberid, :ssid,        student(studentid, :ssid))
        set_roster(year, memberid, :student_id,  student(studentid, :student_id))
        set_roster(year, memberid, :teacher_id,  user(userid, :teacher_id))
        set_roster(year, memberid, :employee_id, user(userid, :employee_id))
        set_roster(year, memberid, :school_id,   row[:schoolid])
        set_roster(year, memberid, :school_code, row[:alternate_school_number])
        set_roster(year, memberid, :grade_level, enrollment(year, studentid, :grade_level))
        set_roster(year, memberid, :period,      period)
        set_roster(year, memberid, :term,        term)
        set_roster(year, memberid, :course_id,   courseid)
        set_roster(year, memberid, :section_id,  sectionid)
      
        num_rows += 1
        tick_message("#{num_rows} roster records analyzed") if num_rows % 100 == 0
      end
    end
  end
  
  def output_files
    roster_fields = [
      :ssid, :student_id, :teacher_id, :employee_id, 
      :school_id, :school_code, :grade_level, :period, :term, :course_id, :section_id ]
      
    course_keys = { }
        
    ::File.makedirs(@output_dir) unless ::File.directory?(@output_dir)
    
    years = @rosters.keys.sort { |a,b| b <=> a }
    years.each do |year|
      fname = "#{year}rosters.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = roster_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        members = @rosters[year].keys.sort
        members.each do |memberid|
          # mark courses
          courseid = roster(year, memberid, :course_id)
          course_keys[courseid] = 1
          values = roster_fields.collect { |f| roster(year, memberid, f) }.join("\t")
          out.write("#{values}\n")
          num_rows += 1
          tick_message("#{num_rows} roster records written for #{year}") if num_rows % 100 == 0
        end
      end
      
      user_fields = [ :employee_id, :teacher_id, :school_id, :school_code, 
        :first_name, :last_name, :email_address ]
      fname = "#{year}users.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = user_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        teachers = @teacher_years[year].keys
        teachers.each do |userid|
          next if user(userid, :school_code) == 0
          values = user_fields.collect { |f| user(userid, f) }.join("\t")
          out.write("#{values}\n")
          num_rows += 1
          tick_message("#{num_rows} teacher records written for #{year}") if num_rows % 100 == 0
        end
      end

      demo_fields = [ :ssid, :student_id, :school_code, :first_name, :last_name, 
        :birthdate, :gender, :parent, :street, :city, :state,  :zip, :phone_number,
        :primary_language, :ethnicity, :language_fluency,
        :date_entered_school, :date_entered_district, :first_us_entry_date,
        :gate, :primary_disability, :nslp, :parent_education, :migrant_ed,
        :date_rfep, :special_program, :title_1 ]
      fname = "#{year}demo.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = demo_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        if @enrollments[year]
          students = @enrollments[year].keys
          students.each do |studentid|
            ssid = student(studentid, :ssid)
            next if ssid.nil? || ssid.empty?
            set_student(studentid, :school_id,   enrollment(year, studentid, :school_id))
            set_student(studentid, :school_code, enrollment(year, studentid, :school_code))
            values = demo_fields.collect { |f| student(studentid, f) }.join("\t")
            out.write("#{values}\n")
            num_rows += 1
            tick_message("#{num_rows} demographic records written for #{year}") if num_rows % 100 == 0
          end
        end
      end
    end
    
    # note: can we do subject mapping?
    course_fields = [ :course_id, :abbreviation, :name,
      :credits, :subject_code, :a_to_g, :school_id, :school_code ]
    fname = "courses.txt"
    num_rows = 0
    ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
      header_fields = course_fields.collect { |f| f.to_s }.join("\t")
      out.write("#{header_fields}\n")
      course_keys.each_key do |courseid|
        values = course_fields.collect { |f| course(courseid, f) }.join("\t")
        out.write("#{values}\n")
        num_rows += 1
        tick_message("#{num_rows} course records written") if num_rows % 100 == 0
      end
    end

    system("zip -j -r #{@output_dir} #{@output_dir}")
    true
  end
  
  def set_course(courseid, key, value)
    (@courses[courseid] ||= { })[key] = value
  end
  
  def course(courseid, key)
    return nil unless @courses.has_key?(courseid)
    @courses[courseid][key]
  end

  def set_user(userid, key, value)
    (@users[userid] ||= { })[key] = value
  end
  
  def user(userid, key)
    return nil unless @users.has_key?(userid)
    @users[userid][key]
  end
  
  def set_student(studentid, key, value)
    (@students[studentid] ||= { })[key] = value
  end
  
  def student(studentid, key)
    return nil unless @students.has_key?(studentid)
    @students[studentid][key]
  end
  
  def set_enrollment(year, studentid, key, value)
    ((@enrollments[year] ||= { })[studentid] ||= { })[key] = value
  end
  
  def enrollment(year, studentid, key)
    return nil unless @enrollments.has_key?(year)
    return nil unless @enrollments[year].has_key?(studentid)
    @enrollments[year][studentid][key]
  end
  
  def set_teacher_year(year, userid, key, value)
    ((@teacher_years[year] ||= { })[userid] ||= { })[key] = value
  end
  
  def teacher_year(year, userid, key)
    return nil unless @teacher_years.has_key?(year)
    return nil unless @teacher_years[year].has_key?(userid)
    @teacher_years[year][userid][key]
  end

  def set_roster(year, memberid, key, value)
    ((@rosters[year] ||= { })[memberid] ||= { })[key] = value
  end
  
  def roster(year, memberid, key)
    return nil unless @rosters.has_key?(year)
    return nil unless @rosters[year].has_key?(memberid)
    @rosters[year][memberid][key]
  end
  
  def process_for_single_year
    tick_message("Analyzing course data")
    analyze_course_data
    tick_message("Analyzing teacher data", 10)
    analyze_user_data(@single_year)
    tick_message("Analyzing student demographic data", 10)
    analyze_student_data(@single_year)
    tick_message("Analyzing student race data", 10)
    analyze_race_data
    tick_message("Analyzing student program data", 10)
    analyze_program_data
    tick_message("Analyzing roster data", 10)
    analyze_roster_data
    tick_message("Preparing output files", 10)
    output_files
    tick_message("Output files ready", 10)
  end

  def process_for_all_years
    tick_message("Analyzing course data")
    analyze_course_data
    VALID_YEARS.each do |year|
      tick_message("Analyzing teacher data for #{year}", 10)
      analyze_user_data(year)
      tick_message("Analyzing student demographic data for #{year}", 10)
      analyze_student_data(year)
    end
    tick_message("Analyzing roster data", 10)
    analyze_roster_data
    tick_message("Preparing output files", 10)
    output_files
    tick_message("Output files ready", 10)
  end
  
  def process_files
    if @single_year.nil?
      process_for_all_years
    else
      process_for_single_year
    end
  end
  
  def expression_to_period(expr)
    expr.nil? ? nil : expr.gsub(/[^0-9].*$/, '').to_i
  end
  
  def term_abbreviation(term_abbr)
    TERM_ABBRS[term_abbr] || term_abbr
  end
  
  def course_abbreviation(name)
    words = name.split
    abbr = words.first[0, 4].upcase.strip
    suffix = (words.size > 1 && (words.last == 'K' || words.last.to_i != 0)) ?
      words.last.upcase : ''
    "#{abbr}#{suffix}"
  end
  
  def parse_date(s)
    m, d, y = s.split(/[-\/]/).collect { |part| part.strip.empty? ? nil : part.to_i }
    if y.nil?
      # no year specified
      if d.nil?
        # raise "unrecognized date format: #{s}"
        # assume y: convert to 1/1/y
        y = m
        m = 1
        d = 1
      else
        # assume m/y: convert to m/1/y
        y = d
        d = 1
      end
    end
    if !m.nil? && m > 1900 
      # assume y/m/d
      t = y
      y = m
      m = d
      d = t
    end
    raise "invalid month" if m.nil? || m < 1 || m > 12
    raise "invalid day" if d.nil? || d < 1 || d > 31
    if !y.nil?
      if y < 20
        y += 2000
      elsif y < 100
        y += 1900
      end
    end
    raise "invalid year" if y.nil? || y < 1940 || y > 2015
    return Date.new(y, m, d)
  end
  
  def date_to_year_abbr(entrydate)
    entrydate = parse_date(entrydate)
    year_number = entrydate.month >= 7 ? entrydate.year-1990 : entrydate.year-1991
    year_number_to_year_abbr(year_number)
  end
  
  def year_abbr_to_term(year)
    sprintf("%02d00", (year.split('-')[0].to_i + 10) % 100)
  end
  
  def term_to_year_abbr(termid)
    year_number_to_year_abbr(termid.to_i/100)
  end
  
  def year_number_to_year_abbr(year_number)
    sprintf("%02d-%02d", (year_number + 90) % 100, (year_number + 91) % 100)
  end
end

DdImporter.new.perform

