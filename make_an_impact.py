import xml.etree.ElementTree as ET
import sql_server_config
import pymssql
import sys

all_fields = set([])
DB_TO_REDCAP_FIELD_MAP = {"FIRSTNAME" : "first_name",
    "LASTNAME" : "last_name",
    "TELEPHONE" : "phone_number",
    "EMAIL" : "email",
    "YOURFIRSTNAME" : "your_first_name",
    "YOURLASTNAME" : "your_last_name",
    "YOURPHONENUMBER" : "your_phone_number",
    "YOUREMAIL" : "your_email",
    "PATIENTFIRSTNAME" : "patient_first_name",
    "PATIENTLASTNAME" : "patient_last_name",
    "YOURRELATIONSHIPPATIENT" : "relationship_to_patient",
    "CANCERTYPE" : "cancer_type",
    "PROVIDEINFORMATIONABOUTDIAGNOSIS" : "info_about_diagnosis",
    "DATEOFDIAGNOSIS" : "date_of_diagnosis",
    "CHEMOTHERAPY" : "chemotherapy",
    "CHEMOTHERAPYDETAILS" : "chemotherapy_details",
    "SURGERY" : "surgery",
    "SURGERYDETAILS" : "surgery_details",
    "CANCERFREE" : "cancer_free"
}

def parse_var(var):
    return var.findall("./string")[0].text.split("|")[1].strip()

def parse_data_field(data):
    fields = {}
    root = ET.fromstring(data)
    # initialize all fields to empty string
    for value in DB_TO_REDCAP_FIELD_MAP.values():
        fields[value] = ""
 
    for record in root.findall("./data/struct"):
        for child in record:
            db_field_name = child.attrib["name"]
            all_fields.add(db_field_name)
            if db_field_name in DB_TO_REDCAP_FIELD_MAP:
                fields[DB_TO_REDCAP_FIELD_MAP[db_field_name]] = parse_var(child)

    if len(fields.keys()) == 0:
        print >> sys.stderr, "ERROR: could not parse '%s'" % (data)
        sys.exit(1)

    return fields

# This is the SQL Server table:
# CREATE TABLE [dbo].[ImpactData](
#   [ID] [nvarchar](100) NOT NULL,
#   [DateEntered] [datetime] NOT NULL,
#   [Data] [ntext] NOT NULL,
#   [FName] [nvarchar](75) NULL,
#   [LName] [nvarchar](75) NULL,
#   [email] [nvarchar](100) NULL,
#   [Confirmation] [nvarchar](50) NULL
#) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

conn = pymssql.connect(server=sql_server_config.server, user=sql_server_config.user, password=sql_server_config.password, database=sql_server_config.database)

cursor = conn.cursor()
cursor.execute("SELECT ID, DateEntered, Data, FName, LName, email, Confirmation FROM ImpactData")
printed_header = False
all_handled_fields = set([])
for row in cursor:
    row = [ value.strip() if type(value) == unicode else value for value in row ]
    # "Data" field, turn back into string from unicode because the XML parser must decode the unicode itself
    fields = parse_data_field(row[2].encode('utf-8')) 
    for key in fields.keys():
        all_handled_fields.add(key)
    if not printed_header:
        print ",".join(["record_id", "date_entered", "confirmation"] + sorted(fields.keys()) + ["make_an_impact_complete"])
        printed_header = True
    if row[3] != fields["first_name"] and row[3] != fields["your_first_name"]:
        print >> sys.stderr, "ERROR: database table field '%s' has value '%s' which does not match parsed field '%s' from 'Data' which has value '%s' or parsed field '%s' which has value '%s'" % \
            ("FName", row[3], "first_name", fields["first_name"], "your_first_name", fields["your_first_name"])
    if row[4] != fields["last_name"] and row[4] != fields["your_last_name"]:
        print >> sys.stderr, "ERROR: database table field '%s' has value '%s' which does not match parsed field '%s' from 'Data' which has value '%s' or parsed field '%s' from 'Data' which has value '%s'" % \
            ("LName", row[4], "last_name", fields["last_name"], "your_last_name", fields["your_last_name"])
    if row[5] != fields["email"] and row[5] != fields["your_email"]:
        print >> sys.stderr, "ERROR: database table field '%s' has value '%s' which does not match parsed field '%s' from 'Data' which has value '%s' or parsed field '%s' from 'Data' which has value '%s'" % \
            ("email", row[5], "email", fields["email"], "your_email", fields["your_email"])

    print "\"" + "\",\"".join([row[0].encode("utf8"), row[1].strftime('%Y-%m-%d %H:%M:%S'), row[6].encode("utf8")] + \
        [fields[k].encode("utf8") for k in sorted(fields.keys())] + ["2"]) + "\""
      

if len(all_fields) - 1 != len(all_handled_fields): # - 1 because there is a field with the list of fields in it
    print >> sys.stderr, "ERROR: len('%s') = %d and len('%s') = %d are not equal" % (",".join(all_fields), 
        (len(all_fields) - 1), ",".join(all_handled_fields), len(all_handled_fields))
