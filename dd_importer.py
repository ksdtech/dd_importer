from __future__ import print_function

from app_config import (
  school_year, source_dir, output_base_dir,
  do_uploads, zip_file_name, username, password,
  sftp_host, sftp_path, webdav_host, webdav_protocol,
  webdav_path, webdav_use_digest_auth 
)

import csv
from datetime import date
import glob
import os
import re
import shutil
import sys
import zipfile

import easywebdav
import pysftp

STUDENTS_HEADERS = [s.strip() for s in '''
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
'''.split('\n')[1:-1]]

TEACHERS_HEADERS = [s.strip() for s in '''
id
teachernumber
schoolid
[39]alternate_school_number
first_name
last_name
email_addr
status
staffstatus
'''.split('\n')[1:-1]]

COURSES_HEADERS = [s.strip() for s in '''
course_number
course_name
credit_hours
credittype
ca_courselevel
schoolid
[39]alternate_school_number
'''.split('\n')[1:-1]]

STUDENT_SCHEDULES_HEADERS = [s.strip() for s in '''
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
'''.split('\n')[1:-1]]

FLUENCY_CODES = {
  'EO': 1,
  'IFEP': 2,
  'EL': 3,
  'RFEP': 4,
  'TBD': 5
}

TERM_ABBRS = {
  '01-02': 'YR',
  '02-03': 'YR',
  '03-04': 'YR',
  '04-05': 'YR',
  '05-06': 'YR',
  '06-07': 'YR',
  '07-08': 'YR',
  '08-09': 'YR',
  '09-10': 'YR',
  '10-11': 'YR',
  '11-12': 'YR',
  '12-13': 'YR',
  '13-14': 'YR',
  '14-15': 'YR',
  '15-16': 'YR',
  '16-17': 'YR',
  'HT 1': 'H1',
  'HT 2': 'H2',
  'HT 3': 'H3',
  'HT 4': 'H4',
  'HT 5': 'H5',
  'HT 6': 'H6',
  'HT1': 'H1',
  'HT2': 'H2',
  'HT3': 'H3',
  'HT4': 'H4',
  'HT5': 'H5',
  'HT6': 'H6'
}

# don't forget to update this every year!
VALID_YEARS = [ 
  '01-02', '02-03', '03-04', '04-05', '05-06',
  '06-07', '07-08', '08-09', '09-10', '10-11',
  '11-12', '12-13', '13-14', '14-15', '15-16', '16-17' 
]
    
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

class DdImporter:
  def __init__(self):
    self.rosters = { }
    self.users = { }
    self.courses = { }
    self.students = { }
    self.enrollments = { }
    self.teacher_years = { }
    self.custom_fields = { }
    
    self.today = date.today()
    self.use_race_file = False
    self.use_program_file = False
    self.data_dir = os.path.realpath(output_base_dir)
    self.input_dir = os.path.realpath(source_dir)
    self.output_dir = os.path.join(self.data_dir, 'datafiles')
    self.archive_dir = os.path.join(self.data_dir, 'archives', self.today.strftime('%Y-%m-%d'))
    self.zip_file_name = zip_file_name

    self.single_school = None
    self.single_year = school_year
    if self.single_year == 'auto':
      year = self.today.year
      if self.today.month > 8 or (self.today.month == 8 and self.today.day >= 15):
        year += 1

      self.single_year = '%d-%d' % (((year-1) % 100), (year % 100))
      print('auto year: %s' % self.single_year)

    if self.single_year and self.single_year not in VALID_YEARS:
      self.single_year = None 

    self.uploads = do_uploads

   
  def perform(self):
    print('Starting job')

    self.process_files()
    if self.output_files():
      if self.uploads:
        self.package_and_archive_files()
        self.upload_file_by_webdav()


  def package_and_archive_files(self):
    print('Zipping files')
    zip_file_path = os.path.join(self.output_dir, self.zip_file_name + '.zip')
    if os.path.exists(zip_file_path):
      os.remove(zip_file_path)
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
      for fname in glob.glob(os.path.join(self.output_dir, "*.txt")):
        # zip without paths '-j'
        print("adding %s to zip file" % fname)
        arcname = os.path.basename(fname)
        zipf.write(fname, arcname)
    
    print('Archiving zip file')
    if not os.path.isdir(self.archive_dir):
      os.makedirs(self.archive_dir)
    shutil.copy(zip_file_path, self.archive_dir)


  def upload_file_by_webdav(self):
    print('Uploading zip file via WebDAV')
    remote_path = self.zip_file_name + '.zip'
    local_path = os.path.join(self.output_dir, self.zip_file_name + '.zip')

    try:
      webdav = easywebdav.connect(webdav_host, protocol=webdav_protocol,
        verify_ssl=True,
        username=username, password=password, path=webdav_path)
      # upload method doesn't give us response information
      # webdav.upload(local_path, remote_path)
      with open(local_path, 'rb') as f:
        resp = webdav._send('PUT', remote_path, (200, 201, 204), data=f)
        print('Upload successful, response was %d' % resp.status_code)
    except Exception as e:
      print('Upload failed: %s' % e)


  def upload_file_by_sftp(self):
    print('Uploading zip file via SFTP')
    local_fname = os.path.join(self.output_dir, self.zip_file_name + '.zip')

    try:
      with pysftp.Connection(sftp_host, username=username, password=password) as sftp:
        with sftp.cd(sftp_path): 
          sftp.put(local_fname)
          print('Upload successful')
    except Exception as e:
      print('Upload failed: %s' % e)


  def process_csv(self, path, hdr_check):
    with open(path, 'r') as f:
      headers = None
      if hdr_check is not None:
        test = f.read(len(hdr_check))
        if test != hdr_check:
          f.seek(0)
          headers = hdr_check
      reader = csv.DictReader(f, fieldnames=headers, delimiter='\t',
        lineterminator='\n', quoting=csv.QUOTE_NONE)

      # change '[39]Alternate School Number' to 'alternate_school_number'
      new_headers = [ ]
      for h in reader.fieldnames:
        h = re.sub(r'[^_a-z0-9]', '', re.sub(r'^[^\]]+\]', '', h.lower().replace(' ', '_')))
        new_headers.append(h)
      reader.fieldnames = new_headers
      for row in reader:
        yield row


  def analyze_student_data(self, year):
    num_rows = 0
    path = os.path.join(self.input_dir, 'dd-students.txt')
    for row in self.process_csv(path, STUDENTS_HEADERS):
      studentid = row['id']
      schoolid = int(row['schoolid'])
      if self.single_school and schoolid != self.single_school:
        print('Skipping student %s; wrong school' % studentid)
        continue

      parent_name = ' '.join([ row['mother_first'], row['mother'] ]).strip()
      if parent_name == '':
        parent_name = ' '.join([ row['father_first'], row['father'] ]).strip()

      self.set_student(studentid, 'ssid',       row['state_studentnumber'])
      self.set_student(studentid, 'student_id', row['student_number'])
      self.set_student(studentid, 'first_name', row['first_name'])
      self.set_student(studentid, 'last_name',  row['last_name'])
      self.set_student(studentid, 'gender',     row['gender'])
      self.set_student(studentid, 'parent',     parent_name)
      self.set_student(studentid, 'street',     row['street'])
      self.set_student(studentid, 'city',       row['city'])
      self.set_student(studentid, 'state',      row['state'])
      self.set_student(studentid, 'zip',        row['zip'])
      self.set_student(studentid, 'phone_number',          row['home_phone'])
      self.set_student(studentid, 'parent_education',      row['ca_parented'])
      self.set_student(studentid, 'birthdate',             self.clean_date(row['dob']))
      self.set_student(studentid, 'date_entered_school',   self.clean_date(row['schoolentrydate']))
      self.set_student(studentid, 'date_entered_district', self.clean_date(row['districtentrydate']))
      self.set_student(studentid, 'first_us_entry_date',   self.clean_date(row['ca_firstusaschooling']))
      self.set_student(studentid, 'date_rfep',             self.clean_date(row['ca_daterfep']))

      if self.use_race_file:
        # Before we update based on race codes, we set the 'primary' ethnicity
        # to '500' if the student is hispanic/latino
        hispanic_ethnicity = ''
        if int(row['fedethnicity']) == 1:
          hispanic_ethnicity = '500'
        self.set_student(studentid, 'ethnicity', hispanic_ethnicity)
      else:
        self.set_student(studentid, 'ethnicity', row['ethnicity'])

      primary_language = row['ca_primarylanguage']
      self.set_student(studentid, 'primary_language',      primary_language)
      
      fluency = row['ca_elastatus']
      if fluency != '':
        fluency = FLUENCY_CODES.get(fluency.upper(), '')
      self.set_student(studentid, 'language_fluency', fluency)
      
      self.set_student(studentid, 'gate',       'N')
      self.set_student(studentid, 'nslp',       'N')
      self.set_student(studentid, 'migrant_ed', 'N')
      self.set_student(studentid, 'special_program', 'N')
      self.set_student(studentid, 'title_1',    'N')
      if not self.use_program_file:
        if re.match(r'Yes', row['ca_gate'], re.I):
          self.set_student(studentid, 'gate',       'Y') 
        # if re.match(r'Yes', ...
        #   self.self.set_student(studentid, 'nslp',       'Y')
        if re.match(r'Yes', row['ca_migranted'], re.I):
          self.set_student(studentid, 'migrant_ed', 'Y')
        # Special Ed
        if row['ca_primdisability'] != '' and row['ca_primdisability'] != '000':
          self.set_student(studentid, 'special_program',   'Y')
          self.set_student(studentid, 'primary_disability', row['ca_primdisability'])

        if row['ca_titlei_targeted']:
          self.set_student(studentid, 'title_1',    'Y') 

      enroll_status = int(row['enroll_status'])
      enroll_year = self.date_to_year_abbr(row['entrydate'])
      # print 'student #{studentid} entrydate #{row['entrydate']} for year #{year}, enroll_year #{enroll_year}, enroll_status #{enroll_status}'
      if enroll_status == 0 or (year == enroll_year and enroll_status > 0):
        self.set_enrollment(year, studentid, 'school_id',   schoolid)
        self.set_enrollment(year, studentid, 'school_code', row['alternate_school_number'])
        self.set_enrollment(year, studentid, 'grade_level', row['grade_level'])
        # print 'enrolled'
      else:
        # print 'skipping enrollment'
        pass

      num_rows += 1
      if num_rows % 100 == 0:
        print('%d student records analyzed' % num_rows)


  def analyze_race_data(self):
    # we bail after we get the first race...
    path = os.path.join(self.input_dir, 'dd-races.txt')
    for row in self.process_csv(path, None):
      studentid = row['studentid']
      if not self.current_student(studentid):
        continue
      race = row['racecd']
      if not self.student(studentid, 'ethnicity'):
        self.set_student(studentid, 'ethnicity', race)


  def nil_date(self, ds):
    return ds == None or ds == '' or ds == '0/0/0' or ds == '01/01/1900'


  def analyze_program_data(self):
    path = os.path.join(self.input_dir, 'dd-programs.txt')
    for row in self.process_csv(path, None):
      studentid = row['foreignkey']
      if not self.current_student(studentid):
        continue

      start_date = row.get('user_defined_date', '')
      if self.nil_date(start_date):
        start_date = self.today
      else:
        start_date = self.parse_date(start_date)
      end_date = row.get('user_defined_date2', '')
      if self.nil_date(end_date):
        end_date = self.today
      else:
        end_date = self.parse_date(end_date)
      if start_date > self.today or end_date < self.today:
        # print 'skipping program record start #{start_date} end #{end_date}'
        continue

      program_code = int(row['user_defined_text'])
      if program_code == 122: # Title 1
        self.set_student(studentid, 'title_1',    'Y')
      elif program_code == 127: # GATE
        self.set_student(studentid, 'gate',       'Y')
      elif program_code == 135: # Migrant
        self.set_student(studentid, 'migrant_ed', 'Y')
      elif program_code == 144: # Special Ed
        disability = None
        # custom has these chars: 
        # either '\x11\x04\x03\x12\x00\x03320' for '320' primary
        # or '\x11\x04\x06\x12\x00\x03280\x11\x04\x03\x12\x00\x03320' for '280' secondary, '320' primary
        m = re.search(r'\x11\x04\x03\x12\x00\x03([0-9]{3})', row['custom'])
        if not m:
          raise Exception('Sped program custom didn\'t match')
        disability = m.group(1)
        self.set_student(studentid, 'special_program',   'Y')
        self.set_student(studentid, 'primary_disability', disability)
      elif program_code == 175: # NSLP
        self.set_student(studentid, 'nslp',       'Y')


  def analyze_user_data(self, year):
    num_rows = 0
    path = os.path.join(self.input_dir, 'dd-teachers.txt')
    for row in self.process_csv(path, TEACHERS_HEADERS):
      userid = row['id']
      teacherid = row['teachernumber']
      
      self.set_user(userid,  'employee_id',   teacherid)
      self.set_user(userid,  'teacher_id',    teacherid)
      self.set_user(userid,  'school_id',     row['schoolid'])
      self.set_user(userid,  'school_code',   row['alternate_school_number'])
      self.set_user(userid,  'first_name',    row['first_name'])
      self.set_user(userid,  'last_name',     row['last_name'])
      self.set_user(userid,  'email_address', row['email_addr'])

      dd_access = row.get('datadirector_access', '')
      
      # current teachers or specified administrators
      if int(row['status']) == 1 and (dd_access == '1' or int(row['staffstatus']) == 1):
        self.set_teacher_year(year, userid, 'active', 'y')
        print('teacher %s active for year %s' % (row['last_name'], year))

      num_rows += 1
      if num_rows % 100 == 0:
        print('%d teacher records analyzed' % num_rows)


  def analyze_course_data(self):
    num_rows = 0
    for fname in ['dd-courses-all.txt', 'dd-courses-bacich.txt', 'dd-courses-kent.txt']:
      path = os.path.join(self.input_dir, fname)
      if not os.path.exists(path):
        continue
      for row in self.process_csv(path, COURSES_HEADERS):
        courseid = row['course_number']
        abbreviation = self.course_abbreviation(row['course_name'])
        self.set_course(courseid, 'course_id',    courseid)
        self.set_course(courseid, 'abbreviation', abbreviation)
        self.set_course(courseid, 'name',         row['course_name'])
        self.set_course(courseid, 'credits',      row['credit_hours'])
        self.set_course(courseid, 'subject_code', row['credittype'])
        self.set_course(courseid, 'a_to_g',       '')
        self.set_course(courseid, 'school_id',    row['schoolid'])
        self.set_course(courseid, 'school_code',  row['alternate_school_number'])
      
        num_rows += 1
        if num_rows % 100 == 0:
          print('%d courses analyzed' % num_rows)
      if fname == 'dd-courses-all.txt':
        break


  def analyze_roster_data(self):
    num_rows = 0
    for fname in ['dd-rosters-all.txt', 'dd-rosters-bacich.txt', 'dd-rosters-kent.txt']:
      path = os.path.join(self.input_dir, fname)
      if not os.path.exists(path):
        continue
      for row in self.process_csv(path, STUDENT_SCHEDULES_HEADERS):
        courseid  = row['course_number']
        if courseid in EXCLUDED_COURSES:
          continue
  
        studentid = row['studentid']
        if not self.current_student(studentid):
          continue
        
        termid = row['termid']
        # reject negative termid's - dropped sections
        if termid == '' or termid[:1] == '-':
          continue
        
        sectionid = row['sectionid']
        # reject negative sectionid's - dropped sections
        if sectionid == '' or sectionid[:1] == '-':
          continue
        
        period = self.expression_to_period(row['expression'])
        if period == '':
          continue
        
        term  = self.term_abbreviation(row['abbreviation'])
        if term == '':
          continue
        
        year = self.term_to_year_abbr(termid)
      
        userid = row['teacherid']
        self.set_teacher_year(year, userid, 'active', 'y')
      
        memberid = '-'.join([ courseid, studentid ])
        self.set_roster(year, memberid, 'ssid',        self.student(studentid, 'ssid'))
        self.set_roster(year, memberid, 'student_id',  self.student(studentid, 'student_id'))
        self.set_roster(year, memberid, 'teacher_id',  self.user(userid, 'teacher_id'))
        self.set_roster(year, memberid, 'employee_id', self.user(userid, 'employee_id'))
        self.set_roster(year, memberid, 'school_id',   row['schoolid'])
        self.set_roster(year, memberid, 'school_code', row['alternate_school_number'])
        self.set_roster(year, memberid, 'grade_level', self.enrollment(year, studentid, 'grade_level'))
        self.set_roster(year, memberid, 'period',      period)
        self.set_roster(year, memberid, 'term',        term)
        self.set_roster(year, memberid, 'course_id',   courseid)
        self.set_roster(year, memberid, 'section_id',  sectionid)
      
        num_rows += 1
        if num_rows % 100 == 0:
          print('%d roster records analyzed' % num_rows) 

      if fname == 'dd-rosters-all.txt':
        break 


  def output_files(self):
    files_written = 0
    print('Preparing output files')
    if os.path.isdir(self.output_dir):
      for path in glob.glob(os.path.join(self.output_dir, "*")):
        os.remove(path)
    else:
      os.makedirs(self.output_dir)

    roster_fields = [
      'ssid', 'student_id', 'teacher_id', 'employee_id', 
      'school_id', 'school_code', 'grade_level', 'period', 'term', 'course_id', 'section_id' ]
      
    course_keys = { }
    years = sorted(self.rosters.keys())
    if len(years) == 0 and self.single_year:
      years.append(self.single_year)
    for year in years:
      if year in self.rosters:
        fname = 'rosters_Kentfield.txt' if self.single_year else ('%srosters.txt' % year)
        num_rows = 0
        path = os.path.join(self.output_dir, fname)
        with open(path, 'w') as out:
          files_written += 1
          header_fields = '\t'.join(roster_fields)
          out.write(header_fields)
          out.write('\n')
          members = sorted(self.rosters[year].keys())
          for memberid in members:
            # mark courses
            courseid = self.roster(year, memberid, 'course_id')
            course_keys[courseid] = 1
            values = '\t'.join([str(self.roster(year, memberid, f)) for f in roster_fields])
            out.write(values)
            out.write('\n')
            num_rows += 1
            if num_rows % 100 == 0:
              print('%d roster records written for %s' % (num_rows, year))

      user_fields = [ 'employee_id', 'teacher_id', 'school_id', 'school_code', 
        'first_name', 'last_name', 'email_address' ]
      fname =  'users_Kentfield.txt' if self.single_year else ('%susers.txt' % year)
      num_rows = 0
      path = os.path.join(self.output_dir, fname)
      with open(path, 'w') as out:
        files_written += 1
        header_fields = '\t'.join(user_fields)
        out.write(header_fields)
        out.write('\n')
        teachers = self.teacher_years[year].keys()
        for userid in teachers:
          if self.user(userid, 'school_code') == 0:
            continue
          values = '\t'.join([str(self.user(userid, f)) for f in user_fields])
          out.write(values)
          out.write('\n')
          num_rows += 1
          if num_rows % 100 == 0:
            print('%d teacher records written for %s' % (num_rows, year))

      demo_fields = [ 'ssid', 'student_id', 'school_code', 'first_name', 'last_name', 
        'birthdate', 'gender', 'parent', 'street', 'city', 'state',  'zip', 'phone_number',
        'primary_language', 'ethnicity', 'language_fluency',
        'date_entered_school', 'date_entered_district', 'first_us_entry_date',
        'gate', 'primary_disability', 'nslp', 'parent_education', 'migrant_ed',
        'date_rfep', 'special_program', 'title_1' ]
      fname =  'demo_Kentfield.txt' if self.single_year else ('%sdemo.txt' % year)
      num_rows = 0
      path = os.path.join(self.output_dir, fname)
      with open(path, 'w') as out:
        files_written += 1
        header_fields = '\t'.join(demo_fields)
        out.write(header_fields)
        out.write('\n')
        if self.enrollments[year]:
          students = self.enrollments[year].keys()
          for studentid in students:
            ssid = self.student(studentid, 'ssid')
            if ssid == '':
              continue
            self.set_student(studentid, 'school_id',   self.enrollment(year, studentid, 'school_id'))
            self.set_student(studentid, 'school_code', self.enrollment(year, studentid, 'school_code'))
            values = '\t'.join([str(self.student(studentid, f)) for f in demo_fields])
            out.write(values)
            out.write('\n')
            num_rows += 1
            if num_rows % 100 == 0:
              print('%d demographic records written for %s' % (num_rows, year))

    # note: can we do subject mapping?
    if len(course_keys) != 0:
      course_fields = [ 'course_id', 'abbreviation', 'name',
        'credits', 'subject_code', 'a_to_g', 'school_id', 'school_code' ]
      fname = 'courses_Kentfield.txt'
      num_rows = 0
      path = os.path.join(self.output_dir, fname)
      with open(path, 'w') as out:
        files_written += 1
        header_fields = '\t'.join(course_fields)
        out.write(header_fields)
        out.write('\n')
        for courseid in course_keys:
          values = '\t'.join([str(self.course(courseid, f)) for f in course_fields])
          out.write(values)
          out.write('\n')
          num_rows += 1
          if num_rows % 100 == 0:
            print('%d course records written' % num_rows) 

    return files_written != 0


  def set_course(self, courseid, key, value):
    if not courseid in self.courses:
      self.courses[courseid] = { }
    self.courses[courseid][key] = value


  def course(self, courseid, key):
    if not courseid in self.courses:
      return ''
    return self.courses[courseid].get(key, '')


  def set_user(self, userid, key, value):
    if not userid in self.users:
      self.users[userid] = { }
    self.users[userid][key] = value


  def user(self, userid, key):
    if not userid in self.users:
      return ''
    return self.users[userid].get(key, '')


  def current_student(self, studentid):
    return studentid in self.students


  def set_student(self, studentid, key, value):
    if not studentid in self.students:
      self.students[studentid] = { }
    self.students[studentid][key] = value


  def student(self, studentid, key):
    if not studentid in self.students:
      return ''
    return self.students[studentid].get(key, '')


  def set_enrollment(self, year, studentid, key, value):
    if not year in self.enrollments:
      self.enrollments[year] = { }
    if not studentid in self.enrollments[year]:
      self.enrollments[year][studentid] = { }
    self.enrollments[year][studentid][key] = value

  def enrollment(self, year, studentid, key):
    if not year in self.enrollments:
      return ''
    if not studentid in self.enrollments[year]:
      return ''
    return self.enrollments[year][studentid].get(key, '')


  def set_teacher_year(self, year, userid, key, value):
    if not year in self.teacher_years:
      self.teacher_years[year] = { }
    if not userid in self.teacher_years[year]:
      self.teacher_years[year][userid] = { }
    self.teacher_years[year][userid][key] = value


  def teacher_year(self, year, userid, key):
    if not year in self.teacher_years:
      return ''
    if not userid in self.teacher_years[year]:
      return ''
    return self.teacher_years[year][userid].get(key, '')


  def set_roster(self, year, memberid, key, value):
    if not year in self.rosters:
      self.rosters[year] = { }
    if not memberid in self.rosters[year]:
      self.rosters[year][memberid] = { }
    self.rosters[year][memberid][key] = value


  def roster(self, year, memberid, key):
    if not year in self.rosters:
      return ''
    if not memberid in self.rosters[year]:
      return ''
    return self.rosters[year][memberid].get(key, '')


  def process_for_single_year(self):
    print('Analyzing course data')
    self.analyze_course_data()
    print('Analyzing teacher data - single year')
    self.analyze_user_data(self.single_year)
    print('Analyzing student demographic data - single year')
    self.analyze_student_data(self.single_year)
    if self.use_race_file:
      print('Analyzing student race data')
      self.analyze_race_data()
    if self.use_program_file:
      print('Analyzing student program data')
      self.analyze_program_data()
    print('Analyzing roster data')
    self.analyze_roster_data()


  def process_for_all_years(self):
    print('Analyzing course data')
    self.analyze_course_data()
    for year in VALID_YEARS:
      print('Analyzing teacher data for #{year}')
      self.analyze_user_data(year)
      print('Analyzing student demographic data for #{year}')
      self.analyze_student_data(year)
    print('Analyzing roster data')
    self.analyze_roster_data()


  def process_files(self):
    if not self.single_year:
      self.process_for_all_years()
    else:
      self.process_for_single_year()


  def expression_to_period(self, expr):
    if expr == '':
      return ''
    period = int(re.sub(r'[^0-9].*$', '', expr))
    if period == 0:
      return ''
    # DD only allows 9 periods
    if period > 9:
      period = 9
    return period


  def term_abbreviation(self, term_abbr):
    return TERM_ABBRS.get(term_abbr, term_abbr)


  def course_abbreviation(self, name):
    words = name.split()
    first_word = words[0].upper()
    abbr = re.sub(r'[^A-Z]', '', first_word)[:4]
    suffix = ''
    if len(words) > 1:
      last_word = words[-1].upper()
      try:
        if re.match(r'K|TK|[1-8]', last_word):
          suffix = last_word
      finally:
        pass
    return abbr + suffix


  def split_date(self, raw_date):
    mo = None
    da = None
    yr = None
    if raw_date != '':
      datestr = re.sub(r'-', '/', raw_date).strip()
      m = re.match(r'(\d+)\/(\d+)\/(\d+)(\s|$)', datestr)
      if m:
        mo = int(m.group(1))
        da = int(m.group(2))
        yr = int(m.group(3))
      else:
        m = re.match(r'(\d+)\/(\d+)(\s|$)', datestr)
        if m:
          mo = int(m.group(1))
          da = 1
          yr = int(m.group(2))
      if mo and da and yr:
        if yr < 20:
          yr += 2000
        elif yr < 100:
          yr += 1900
        if mo < 1 or mo > 12 or da < 1 or da > 31 or yr < 1900 or yr > 2020:
          mo = None
          da = None
          yr = None
    return (mo, da, yr)


  def clean_date(self, raw_date):
    mo, da, yr = self.split_date(raw_date)
    if mo:
      return '%02d/%02d/%04d' % (mo, da, yr)
    return ''


  def parse_date(self, raw_date):
    mo, da, yr = self.split_date(raw_date)
    if mo:
      return date(yr, mo, da)
    return None


  def date_to_year_abbr(self, entrydate):
    entrydate = self.parse_date(entrydate)
    if not entrydate:
      raise Exception('can\'t parse date %s' % entrydate)
    year_number = entrydate.year - 1991
    if entrydate.month >= 7:
      year_number += 1
    return self.year_number_to_year_abbr(year_number)


  def year_abbr_to_term(self, year):
    return '%02d00' % ((int(year.split('-')[0]) + 10) % 100)


  def year_number_to_year_abbr(self, year_number):
    return '%02d-%02d' % (((year_number + 90) % 100, (year_number + 91) % 100))


  def term_to_year_abbr(self, termid):
    return self.year_number_to_year_abbr(int(termid) / 100)


if __name__ == '__main__':
  DdImporter().perform()
