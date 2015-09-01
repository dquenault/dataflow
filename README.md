# dataflow

Very basic attempt at creating a file pump using google dataflows.

Currently uses a dynamically generated schema object based off a file input - however this does not work with the parrallel functions and needs to be corrected.

Possibly using a seperate pipeline to get the schema file and create the object for a subsequent pipeline to write to BQ.
