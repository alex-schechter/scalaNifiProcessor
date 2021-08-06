# ScalaNifiProcessor

This repo is an example of custom nifi processor (version 1.12.1) written in scala

Steps to compile:
1. mvn clean package
2. copy the nar at customFlattenJson-nar/target/customFlattenJson-nar-1.0.nar to the lib dir of nifi at restart.

This processor should flatten nested jsons.

Examples:
****************************************************
```json
{
   "field1": {
      "field2": "some_value"
   }
}
```
will become
```json
{
   "field1_field2": "some_value"
}
```

****************************************************
```json
{
   "field1": {
      "array1": [{
        "name": "value"
      },{
        "name": "value2"
      }]
   }
}
```
will become
```json
[
  {
    "field1_array1_name": "value"
  },
  {
    "field1_array1_name": "value2"
  }
]
```