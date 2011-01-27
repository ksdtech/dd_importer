require 'configatron'
require 'dbi'
require 'oci8'
require 'ftools'

class PsExporter
  QUERIES = {
  :student_query => %{ SELECT st.ID,
  st.Student_Number,
  st.State_StudentNumber,
  st.SchoolID,
  st.First_Name,
  st.Last_Name,
  TO_CHAR(st.DOB, 'MM/DD/YYYY') AS DOB,
  st.FedEthnicity,
  st.Gender,
  st.Enroll_Status,
  st.Grade_Level,
  mf.Value AS Mother_First,
  st.Mother,
  ff.Value AS Father_First,
  st.Father,
  st.Street,
  st.City,
  st.State,
  st.Zip,
  st.Home_Phone,
  TO_CHAR(st.SchoolEntryDate, 'MM/DD/YYYY') AS SchoolEntryDate,
  TO_CHAR(st.DistrictEntryDate, 'MM/DD/YYYY') AS DistrictEntryDate,
  TO_CHAR(st.EntryDate, 'MM/DD/YYYY') AS EntryDate,
  TO_CHAR(st.ExitDate, 'MM/DD/YYYY') AS ExitDate,
  sch.Alternate_School_Number,
  pe.Value AS CA_ParentEd,
  pl.Value AS CA_PrimaryLanguage,
  el.Value AS CA_ELAStatus,
  rfep.Value AS CA_DateRFEP,
  fs.Value AS CA_FirstUSASchooling
  FROM Students st
  LEFT OUTER JOIN Schools sch ON sch.School_Number=st.SchoolID
  LEFT OUTER JOIN CustomText mf ON (mf.FieldNo={{Mother_First}} AND mf.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText ff ON (ff.FieldNo={{Father_First}} AND ff.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText pe ON (pe.FieldNo={{CA_ParentEd}} AND pe.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText pl ON (pl.FieldNo={{CA_PrimaryLanguage}} AND pl.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText el ON (el.FieldNo={{CA_ELAStatus}} AND el.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText rfep ON (rfep.FieldNo={{CA_DateRFEP}} AND rfep.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText fs ON (fs.FieldNo={{CA_FirstUSASchooling}} AND fs.KeyNo=st.DCID) }, 
  
  :teacher_query => %{ 
  SELECT t.ID, t.TeacherNumber, t.SchoolID, 
  sch.Alternate_School_Number, 
  t.First_Name, t.Last_Name, t.Email_Addr,
  t.Status, t.StaffStatus 
  FROM Teachers t
  LEFT OUTER JOIN Schools sch ON sch.School_Number=t.schoolID },
  
  :school_query => %{ SELECT sch.Name, sch.School_Number,
  sch.Low_Grade, sch.High_Grade, sch.Alternate_School_Number
  FROM Schools sch }, 
  
  :course_query => %{ SELECT c.Course_Number, c.Course_Name, c.Credit_Hours, 
  c.CreditType, cl.Value AS CA_CourseLevel, 
  c.SchoolID, sch.Alternate_School_Number
  FROM Courses c 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=c.SchoolID 
  LEFT OUTER JOIN CustomText cl ON (cl.FieldNo={{CA_CourseLevel:300}} AND cl.KeyNo=c.ID) },
  
  :roster_query => %{ SELECT 
  cc.StudentID,
  cc.TeacherID, cc.SchoolID, cc.TermID, 
  st.State_StudentNumber, f.TeacherNumber, 
  sch.Alternate_School_Number, st.Grade_Level, 
  cc.Expression, t.Abbreviation, 
  cc.Course_Number, cc.Section_Number, cc.SectionID 
  FROM cc
  LEFT OUTER JOIN Students st ON st.ID=cc.StudentID 
  LEFT OUTER JOIN Teachers f ON f.ID=cc.TeacherID 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=cc.SchoolID 
  LEFT OUTER JOIN Terms t ON (t.ID=ABS(cc.TermID)) },

  :roster_query_reenrollments => %{ SELECT
  cc.StudentID, 
  cc.TeacherID, cc.SchoolID, cc.TermID,
  st.State_StudentNumber, f.TeacherNumber,
  sch.Alternate_School_Number, re.Grade_Level, 
  cc.Expression, t.Abbreviation, 
  cc.Course_Number, cc.Section_Number, cc.SectionID 
  FROM cc
  LEFT OUTER JOIN Students st ON st.ID=cc.StudentID 
  LEFT OUTER JOIN Teachers f ON f.ID=cc.TeacherID 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=cc.SchoolID 
  LEFT OUTER JOIN Terms t ON (t.ID=ABS(cc.TermID))
  LEFT OUTER JOIN ReEnrollments re ON 
  (re.StudentID=st.ID AND re.EntryDate<=cc.DateEnrolled AND re.ExitDate>=cc.DateLeft) },

  :reenrollment_query => %{ SELECT 
  re.StudentID,
  st.State_StudentNumber, re.SchoolID,
  sch.Alternate_School_Number, re.Grade_Level, 
  TO_CHAR(re.EntryDate, 'MM/DD/YYYY') AS EntryDate, 
  TO_CHAR(re.ExitDate, 'MM/DD/YYYY') AS ExitDate
  FROM ReEnrollments re
  LEFT OUTER JOIN Students st ON st.ID=re.StudentID
  LEFT OUTER JOIN Schools sch ON sch.School_Number=re.SchoolID },

  :program_query => %{ SELECT
  FOREIGNKEY,
  CUSTOM,
  USER_DEFINED_TEXT,
  USER_DEFINED_TEXT2,
  TO_CHAR(USER_DEFINED_DATE, 'MM/DD/YYYY') AS USER_DEFINED_DATE,
  TO_CHAR(USER_DEFINED_DATE2, 'MM/DD/YYYY') AS USER_DEFINED_DATE2
  FROM VirtualTablesData2
  WHERE RELATED_TO_TABLE='StudentProgram' },
  
  :race_query => %{ SELECT
  STUDENTID, RACECD FROM StudentRace } 
  
  }
  
  # don't forget to update this every year!
  VALID_YEARS = 
    [ '01-02', '02-03', '03-04', '04-05', '05-06',
      '06-07', '07-08', '08-09', '09-10', '10-11',
      '11-12', '12-13' ]
  
  def name
    "PowerSchool DataDirector Export"
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
    output_base_dir = options['exporter']['output_base_dir'] or raise "No output_base_dir specified"

    @single_year = options['exporter']['year']
    @single_year = nil if @single_year && !VALID_YEARS.include?(@single_year)
    
    @data_dir = File.expand_path(output_base_dir)
    @input_dir = File.join(@data_dir, 'psexport')
    
    @db_config = @options['oracle_powerschool']
    @custom_fields = { }
    
    @working_at = 0
    @total_work_units = 10 * (@single_year ? 11 : 9 + 2*VALID_YEARS.size)
    run_powerschool_queries
  end
  
  def year_abbr_to_term(year)
    sprintf("%02d00", (year.split('-')[0].to_i + 10) % 100)
  end
  
  # can raise exception
  def custom_field_number(field)
    field, fileno = field.split(/:/)
    fileno = 100 unless fileno
    key = "#{field.downcase}:#{fileno}"
    if !@custom_fields.key?(field)
      field_id = 0
      sql = "SELECT ID FROM FieldsTable WHERE FileNo=#{fileno} AND REGEXP_LIKE(Name,'^#{field}$','i')"
      row = @dbh.select_one(sql)
      field_id = row[0].to_i if row
      @custom_fields[key] = field_id
    end
    @custom_fields[key]
  end

  # can raise exception
  def run_query(query_name, fname, min_termid=nil)
    puts "processing #{query_name}..."
    num_rows = 0
    sql = QUERIES[query_name].gsub(/\{\{([^}]+)\}\}/) do |field|
      custom_field_number($1)
    end
    if query_name == :roster_query && !min_termid.nil?
      sql << " WHERE ABS(cc.TermID)>=#{min_termid}"
    end
    f = nil
    sth = @dbh.execute(sql)
    while row = sth.fetch do
      if f.nil?
        f = ::File.open(fname, "w")
        f.write row.column_names.join("\t")
        f.write "\n"
      end
      vals = [ ] 
      row.each_with_name do |val, name|
        if val.is_a?(Float) || val.is_a?(BigDecimal)
          val = val.to_i 
        else
          if val.is_a?(OCI8::BFILE) || val.is_a?(OCI8::BLOB) || val.is_a?(OCI8::CLOB)
            val = val.read
          end
          val = val.to_s.gsub(/"/, "'")
        end
        vals << val
      end
      f.write vals.join("\t")
      f.write "\n"
      num_rows += 1
      tick_message("Processed #{num_rows} rows in #{query_name}") if num_rows % 100 == 0
    end
    puts " #{num_rows} rows written to #{fname}"
    sth.finish
    f.close if f
    !f.nil?
  end

  # can raise exception
  def connect_db
    return if @dbh
    dsn = "dbi:#{@db_config['adapter']}:#{@db_config['database']}"
    @dbh = DBI.connect(dsn, @db_config['user'], @db_config['password'])
  end
  
  def disconnect_db
    return unless @dbh
  end
  
  # 6 work units
  def run_powerschool_queries
    connect_db
    if @dbh
      min_termid = @single_year.nil? ? nil : year_abbr_to_term(@single_year)
      ::File.makedirs(@input_dir) unless ::File.directory?(@input_dir)

      tick_message("Querying student program data", 10)
      run_query(:program_query, "#{@input_dir}/dd-programs.txt")
      tick_message("Querying student race/ethnicity data", 10)
      run_query(:race_query, "#{@input_dir}/dd-races.txt")
      tick_message("Querying student demographic and current enrollment data")
      run_query(:student_query, "#{@input_dir}/dd-students.txt")
      tick_message("Querying school data", 10)
      run_query(:school_query,  "#{@input_dir}/dd-schools.txt")
      tick_message("Querying course data", 10)
      course_file = run_query(:course_query,  "#{@input_dir}/dd-courses-all.txt")
      system("rm #{@input_dir}/dd-courses-{bacich,kent}.txt") if course_file
      tick_message("Querying prior enrollment data", 10)
      run_query(:reenrollment_query, "#{@input_dir}/dd-reenrollments.txt")
      tick_message("Querying teacher data", 10)
      run_query(:teacher_query, "#{@input_dir}/dd-teachers.txt")
      tick_message("Querying student demographic data", 10)
      roster_file = run_query(:roster_query,  "#{@input_dir}/dd-rosters-all.txt", min_termid)
      system("rm #{@input_dir}/dd-rosters-{bacich,kent}.txt") if roster_file
      tick_message("All data extracted", 10)
  
      disconnect_db
    else
      raise "unable to connect"
    end
  end
end

