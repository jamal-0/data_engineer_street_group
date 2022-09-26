import apache_beam as beam
import json

def Split(element):
  return [element.split(',')]


def full_address(element):
  full_address = " ".join([element[3], *element[7:10]])
  return (full_address, [*element[:3], " ".join(element[4:7]), " ".join(element[10:14]), " ".join(element[14:])])

def convert_to_object(element):
  obj = {
    element[0]:{
      "Transaction_ID":[element[1][x][0] for x in range(len(element[1]))],
      "Transaction_Amount":[element[1][x][1] for x in range(len(element[1]))],
      "Transaction_Date":[element[1][x][2] for x in range(len(element[1]))],
      "Property_Info":element[1][0][3],
      "Property_Address":element[1][0][4],
      "Transaction_Info":[element[1][x][5] for x in range(len(element[1]))]
    }
  }
  return obj


class CompositeTransform(beam.PTransform):

  def expand(self, input_data):

    return (
        input_data
                | "make full address/property ID">> beam.Map(full_address)
                | "group by property ID" >> beam.GroupByKey()
                | "convert to object" >> beam.Map(convert_to_object)

    )

with beam.Pipeline() as pipeline:
  input_data = (pipeline
                | "read from csv">> beam.io.ReadFromText('/Users/jamalkarim/Documents/test_street_group.csv')
                | "split the rows" >> beam.ParDo(Split)
  )
  
  transform_data = (input_data
                | "composite transform" >> CompositeTransform()
  )
  
  output_data = (transform_data
                | "to json" >> beam.Map(json.dumps)
  ) 