require 'rubygems'
require 'configatron'
require 'fastercsv'
require 'ftools'
require 'net/https'
require 'net/sftp'
require 'net_digest_auth'
require 'uri'

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
    ca_primarylanguage
    ca_elastatus
    ca_daterfep
    ca_firstusaschooling
    ca_gate
    ca_migranted
    ca_primdisability
    ca_titlei_targeted
    ethnicity
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
    '12-13' => 'YR',
    '13-14' => 'YR',
    '14-15' => 'YR',
    '15-16' => 'YR',
    '16-17' => 'YR',
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
      '11-12', '12-13', '13-14', '14-15', '15-16', '16-17' ]
      
  EXCLUDED_COURSES = [ 
    'AAAA', 'oooo', # Attendance
    '3100', '4100'  # Bacich Math
  ] 
  
  NON_EXCLUDED_COURSES = [
    '0500', '1500', '2500', '3500', '4500', # Bacich Library
    '0820', '1820', '2820', '3820', '4820', # Bacich Art
    '0830', '1830', '2830', '3830', '4830', # Bacich Tech
    '0880', '1880', '2880', '3880', '4880', # Bacich Music
    '0881', '1881', '2881', '3881', '4881', # Bacich Chorus
    '0700', '1700', '2700', '3700', '4700', # Bacich PE
  ]
  
  def name
    "DataDirector Upload"
  end
  
  def options
    @options ||= configatron.configure_from_yaml('./app_config.yml')
  end
  
  def at(progress, total, message)
    puts "at #{progress}/#{total}: #{message}"
  end
  
  def tick_message(message, incr=0)
    @working_at += incr if incr > 0
    at(@working_at, @total_work_units, message)
  end
  
  def perform
    @rosters = { }
    @users = { }
    @courses = { }
    @students = { }
    @enrollments = { }
    @teacher_years = { }
    @custom_fields = { }
    
    input_dir = options['exporter']['input_dir'] # can be nil
    output_base_dir = options['exporter']['output_base_dir'] or raise "No output_base_dir specified"
    
    @use_race_file = options['exporter']['use_race_file']
    @use_program_file = options['exporter']['use_program_file']
    @data_dir = File.expand_path(output_base_dir)
    @input_dir = input_dir ? File.expand_path(input_dir) : File.join(@data_dir, 'psexport')
    @output_dir = File.join(@data_dir, 'datafiles')
    @archive_dir = File.join(@data_dir, 'archives', Date.today.strftime("%Y-%m-%d"))
    @zip_file_name = options['exporter']['zip_file_name']
    
    @single_year = options['exporter']['year']
    if @single_year == 'auto'
      d = Date.today
      year = d.year
      if d.month > 8 || (d.month == 8 && d.day >= 15)
        year += 1
      end
      @single_year = "#{(year-1) % 100}-#{year % 100}"
      puts "auto year: #{@single_year}"
    end
    @single_year = nil if @single_year && !VALID_YEARS.include?(@single_year)

    @single_school = options['exporter']['school']
        
    @uploads = true
    if options['exporter'].key?('uploads')
      @uploads = options['exporter']['uploads']
    end
    
    # puts "single_year: #{@single_year}"
    # puts "single_school: #{@single_school}"
    # puts "uploads: #{@uploads}"
    
    @working_at = 0
    @total_work_units = 10 * (@single_year ? 11 : 9 + 2*VALID_YEARS.size)
    tick_message("Starting job")
    
    process_files
    if output_files
      if @uploads
        package_and_archive_files
        upload_file_by_webdav
      end
    end
  end
  
  def package_and_archive_files
    tick_message("Zipping files", 10)
    system("rm -f #{@output_dir}/#{@zip_file_name}.zip")
    system("zip -j #{@output_dir}/#{@zip_file_name} #{@output_dir}/*.txt")
    
    tick_message("Archiving zip file", 10)
    ::File.makedirs(@archive_dir) unless ::File.directory?(@archive_dir)
    system("cp #{@output_dir}/#{@zip_file_name}.zip #{@archive_dir}")
  end
  
  def upload_file_by_sftp
    begin
      Net::SFTP.start(options['sftp']['host'], options['sftp']['username'], :password => options['sftp']['password']) do |sftp|
        puts "Connecting via SFTP to #{options['sftp']['host']}"
        sftp.upload!("#{@output_dir}/#{@zip_file_name}.zip", "#{options['sftp']['path']}#{@zip_file_name}.zip")
        puts "File uploaded"
      end
    rescue
      puts "SFTP error: #{$!}"
    end
  end
  
  def upload_file_by_webdav
    tick_message("Uploading zip file", 10)
    zip_contents = File.open("#{@output_dir}/#{@zip_file_name}.zip").read rescue nil
    if zip_contents && !zip_contents.empty?
      url = URI.parse(options['webdav']['url'])
      # puts "scheme #{url.scheme}"
      conn = Net::HTTP.new(url.host, url.port)
      if url.scheme == 'https'
        conn.use_ssl = true
      end
      begin
        conn.start do |http|
          puts "Connecting via WebDAV to #{url.host}:#{url.port}"
          req = Net::HTTP::Delete.new("#{url.path}#{@zip_file_name}.zip")
          if options['webdav']['use_digest_auth']
            # puts "digest auth"
            # try putting something so
            # the server will return a www-authenticate header
            res = http.put(url.request_uri, 'hello') 
            req.digest_auth(options['webdav']['username'], options['webdav']['password'], res)
          else
            # puts "basic auth"
            req.basic_auth(options['webdav']['username'], options['webdav']['password'])
          end
          res = http.request(req)
          puts "WebDAV delete response: #{res.code} #{res.message}"
          # 201 created
          # 204 no content
        end
        conn.start do |http|
          req = Net::HTTP::Put.new("#{url.path}#{@zip_file_name}.zip")
          if options['webdav']['use_digest_auth']
            # puts "digest auth"
            # try putting something so
            # the server will return a www-authenticate header
            res = http.put(url.request_uri, 'hello') 
            req.digest_auth(options['webdav']['username'], options['webdav']['password'], res)
          else
            # puts "basic auth"
            req.basic_auth(options['webdav']['username'], options['webdav']['password'])
          end
          res = http.request(req, zip_contents)
          puts "WebDAV put response: #{res.code} #{res.message}"
          # 201 created
          # 204 no content
          if res.code.to_s =~ /200|201|204/
            puts "success"
            puts "removing zip file"
            system("rm -f #{@output_dir}/#{@zip_file_name}.zip")
          end
        end
      rescue
        puts "WebDAV error: #{$!}"
      end
    else
      raise "Could not read zip file for upload"
    end
  end  
  
  def process_csv(path, hdr_check, opt_headers, &block)
    has_header = File.open(path, "r") { |f| f.read(hdr_check.length) } == hdr_check
    options = {
      :col_sep => "\t", 
      :row_sep => "\n",
      :headers => has_header ? true : opt_headers
    }
    FasterCSV.open(path, options) do |csv|
      # '[39]Alternate School Number' => :alternate_school_number
      csv.header_convert { |h| h.downcase.tr(" ", "_").gsub(/^[^\]]+\]/,'').delete("^a-z0-9_").to_sym }
      csv.each(&block)
    end
  end

  def analyze_student_data(year)
    num_rows = 0
    process_csv("#{@input_dir}/dd-students.txt", 'ID', STUDENTS_HEADERS) do |row|
      studentid = row[:id]
      schoolid = row[:schoolid].to_i
      if @single_school && schoolid != @single_school
        puts "skipping student #{studentid}; wrong school"
        next
      end
      # puts "student #{studentid}"
      
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
      set_student(studentid, :birthdate,             clean_date(row[:dob]))
      set_student(studentid, :date_entered_school,   clean_date(row[:schoolentrydate]))
      set_student(studentid, :date_entered_district, clean_date(row[:districtentrydate]))
      set_student(studentid, :first_us_entry_date,   clean_date(row[:ca_firstusaschooling]))
      set_student(studentid, :date_rfep,             clean_date(row[:ca_daterfep]))

      if @use_race_file
        # Before we update based on race codes, we set the "primary" ethnicity
        # to '500' if the student is hispanic/latino
        hispanic_ethnicity = (row[:fedethnicity].to_i == 1) ? '500' : nil
        set_student(studentid, :ethnicity,             hispanic_ethnicity)
      else
        set_student(studentid, :ethnicity, row[:ethnicity])
      end

      primary_language = row[:ca_primarylanguage]
      primary_language = primary_language.to_i unless primary_language.nil?
      set_student(studentid, :primary_language,      primary_language.to_s)
      
      fluency = row[:ca_elastatus]
      fluency = FLUENCY_CODES.fetch(fluency.upcase, nil) unless fluency.nil?
      set_student(studentid, :language_fluency,      fluency)
      
      set_student(studentid, :gate,       'N')
      set_student(studentid, :nslp,       'N')
      set_student(studentid, :migrant_ed, 'N')
      set_student(studentid, :special_program, 'N')
      set_student(studentid, :title_1,    'N')
      if !@use_program_file
        set_student(studentid, :gate,       'Y') if row[:ca_gate] =~ /Yes/i
        # set_student(studentid, :nslp,       'Y') if ?
        set_student(studentid, :migrant_ed, 'Y') if row[:ca_migranted] =~ /Yes/i
        if row[:ca_primdisability] && row[:ca_primdisability] != '000' # Special Ed
          set_student(studentid, :special_program,   'Y')
          set_student(studentid, :primary_disability, row[:ca_primdisability])
        end
        set_student(studentid, :title_1,    'Y') if row[:ca_titlei_targeted]
      end

      enroll_status = row[:enroll_status].to_i
      enroll_year = date_to_year_abbr(row[:entrydate])
      # puts "student #{studentid} entrydate #{row[:entrydate]} for year #{year}, enroll_year #{enroll_year}, enroll_status #{enroll_status}"
      if enroll_status == 0 || (year == enroll_year && enroll_status > 0)
        set_enrollment(year, studentid, :school_id,   schoolid)
        set_enrollment(year, studentid, :school_code, row[:alternate_school_number])
        set_enrollment(year, studentid, :grade_level, row[:grade_level])
        # puts "enrolled"
      else
        # puts "skipping enrollment"
      end
      num_rows += 1
      tick_message("#{num_rows} student records analyzed") if num_rows % 100 == 0
    end
  end

  def analyze_race_data
    # we bail after we get the first race...
    process_csv("#{@input_dir}/dd-races.txt", 'STUDENTID', true) do |row|
      studentid = row[:studentid]
      next unless current_student?(studentid)
      race = row[:racecd]
      set_student(studentid, :ethnicity, race) unless student(studentid, :ethnicity)
    end
  end
  
  def nil_date?(ds)
    ds.nil? || ds == '0/0/0' || ds == '01/01/1900'
  end
  
  def analyze_program_data
    process_csv("#{@input_dir}/dd-programs.txt", 'FOREIGNKEY', true) do |row|
      studentid = row[:foreignkey]
      next unless current_student?(studentid)

      start_date = row[:user_defined_date]
      start_date = nil if nil_date?(start_date)
      start_date = parse_date(start_date) unless start_date.nil?
      end_date = row[:user_defined_date2]
      end_date = nil if nil_date?(end_date)
      end_date = parse_date(end_date) unless end_date.nil?
      today = Date.today
      unless start_date <= today && (end_date.nil? || end_date >= today)
        # puts "skipping program record start #{start_date} end #{end_date}"
        next
      end
      
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
        # custom has these chars: 
        # either "\x11\x04\x03\x12\x00\x03320" for '320' primary
        # or "\x11\x04\x06\x12\x00\x03280\x11\x04\x03\x12\x00\x03320" for '280' secondary, '320' primary
        m = row[:custom].match(/\x11\x04\x03\x12\x00\x03([0-9]{3})/)  
        raise "custom didn't match" unless m
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
    process_csv("#{@input_dir}/dd-teachers.txt", 'ID', TEACHERS_HEADERS) do |row|
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
        puts "teacher #{row[:last_name]} active for year #{year}"
      end
      
      num_rows += 1
      tick_message("#{num_rows} teacher records analyzed") if num_rows % 100 == 0
    end
  end
  
  def analyze_course_data
    num_rows = 0
    ['dd-courses-all.txt', 'dd-courses-bacich.txt', 'dd-courses-kent.txt'].each do |fname|
      fpath = "#{@input_dir}/#{fname}"
      next unless File.exist?(fpath)
      process_csv(fpath, 'COURSE_NUMBER', COURSES_HEADERS) do |row|
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
      break if fname == 'dd-courses-all.txt'
    end
  end
  
  def analyze_roster_data
    num_rows = 0
    ['dd-rosters-all.txt', 'dd-rosters-bacich.txt', 'dd-rosters-kent.txt'].each do |fname|
      fpath = "#{@input_dir}/#{fname}"
      next unless File.exist?(fpath)
      process_csv(fpath, 'STUDENTID', STUDENT_SCHEDULES_HEADERS) do |row|
        courseid  = row[:course_number]
        next if EXCLUDED_COURSES.include?(courseid)
  
        studentid = row[:studentid]
        next unless current_student?(studentid)
        
        termid = row[:termid]
        # reject negative termid's - dropped sections
        next if termid.nil? || termid[0,1] == '-'
        
        sectionid = row[:sectionid]
        # reject negative sectionid's - dropped sections
        next if sectionid.nil? || sectionid[0,1] == '-'
        
        period = expression_to_period(row[:expression])
        next if period.nil?
        
        term   = term_abbreviation(row[:abbreviation])
        next if term.nil?
        
        memberid = "#{courseid}-#{studentid}"
        year = term_to_year_abbr(termid)
      
        userid = row[:teacherid]
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
      break if fname == 'dd-rosters-all.txt'
    end
  end
  
  def output_files
    files_written = 0
    tick_message("Preparing output files", 10)
    if ::File.directory?(@output_dir)
      system("rm -f #{@output_dir}/*")
    else
      ::File.makedirs(@output_dir)
    end
      
    roster_fields = [
      :ssid, :student_id, :teacher_id, :employee_id, 
      :school_id, :school_code, :grade_level, :period, :term, :course_id, :section_id ]
      
    course_keys = { }
        
    years = @rosters.keys.sort { |a,b| b <=> a }
    years << @single_year if years.empty? && @single_year
    years.each do |year|
      if @rosters[year]
        fname = @single_year ? "rosters_Kentfield.txt" : "#{year}rosters.txt"
        num_rows = 0
        ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
          files_written += 1
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
      end
      
      user_fields = [ :employee_id, :teacher_id, :school_id, :school_code, 
        :first_name, :last_name, :email_address ]
      fname =  @single_year ? "users_Kentfield.txt" : "#{year}users.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        files_written += 1
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
      fname =  @single_year ? "demo_Kentfield.txt" : "#{year}demo.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        files_written += 1
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
    unless course_keys.empty?
      course_fields = [ :course_id, :abbreviation, :name,
        :credits, :subject_code, :a_to_g, :school_id, :school_code ]
      fname = "courses_Kentfield.txt"
      num_rows = 0
      ::File.open("#{@output_dir}/#{fname}", 'w') do |out|
        files_written += 1
        header_fields = course_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        course_keys.each_key do |courseid|
          values = course_fields.collect { |f| course(courseid, f) }.join("\t")
          out.write("#{values}\n")
          num_rows += 1
          tick_message("#{num_rows} course records written") if num_rows % 100 == 0
        end
      end
    end

    files_written != 0
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
  
  def current_student?(studentid)
    @students.has_key?(studentid)
  end
  
  def set_student(studentid, key, value)
    (@students[studentid] ||= { })[key] = value
  end
  
  def student(studentid, key)
    return nil unless current_student?(studentid)
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
    tick_message("Analyzing teacher data - single year", 10)
    analyze_user_data(@single_year)
    tick_message("Analyzing student demographic data - single year", 10)
    analyze_student_data(@single_year)
    if @use_race_file
      tick_message("Analyzing student race data", 10)
      analyze_race_data
    end
    if @use_program_file
      tick_message("Analyzing student program data", 10)
      analyze_program_data
    end
    tick_message("Analyzing roster data", 10)
    analyze_roster_data
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
  end
  
  def process_files
    if @single_year.nil?
      process_for_all_years
    else
      process_for_single_year
    end
  end
  
  def expression_to_period(expr)
    return nil if expr.nil?
    period = expr.gsub(/[^0-9].*$/, '').to_i
    return nil if period == 0
    # DD only allows 9 periods
    period > 9 ? 9 : period
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
  
  def split_date(raw_date)
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
        unless mo >= 1 && mo <= 12 && da >= 1 && da <= 31 && yr > 1900 && yr < 2020
          mo = nil
          da = nil
          yr = nil
        end
      end
    end
    [mo, da, yr]
  end
  
  def clean_date(raw_date)
    mo, da, yr = split_date(raw_date)
    mo.nil? ? nil : sprintf("%02d/%02d/%04d", mo, da, yr)
  end
  
  def parse_date(raw_date)
    mo, da, yr = split_date(raw_date)
    mo.nil? ? nil : Date.new(yr, mo, da)
  end
  
  def date_to_year_abbr(entrydate)
    entrydate = parse_date(entrydate)
    raise "can't parse '#{entrydate}'" unless entrydate
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



