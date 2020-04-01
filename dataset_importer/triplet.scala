// important : this file must be imported with :load and not copy pasted in spark shell
// due to https://stackoverflow.com/a/32312984

case class Triplet(
  subj: String,
  rel: String,
  obj: String)
