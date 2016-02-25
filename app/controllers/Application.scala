package controllers

import scala.collection.JavaConversions._
import play.api._
import play.api.mvc._
import play.api.libs.json._
import scala.collection.generic.MutableMapFactory
import scala.collection.mutable.HashMap
import scala.io.Source
import java.io.FileWriter
import com.hp.hpl.jena.query.QueryExecutionFactory
import org.apache.jena.atlas.json.JsonParseException
import com.hp.hpl.jena.sparql.util.FmtUtils
import com.hp.hpl.jena.query.QueryParseException
import java.io.File
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.net.URLEncoder
import scala.sys.SystemProperties
import com.hp.hpl.jena.sparql.function.library.min
import play.api.Play.current
import play.api.libs.ws._
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.regex.Pattern
import org.hjson.JsonValue
import org.hjson.Stringify
import org.apache.jena.atlas.web.HttpException

object Application extends Controller {

  val servicePrefix = new SystemProperties().getOrElse("service.prefix", "http://demo.seco.tkk.fi/arpa/")

  val analyzeWS = WS.url(new SystemProperties().getOrElse("analyze.address", "http://demo.seco.tkk.fi/las/analyze"))

  case class Extractor(id: String,
                       val name: String,
                       val endpointURL: String,
                       val lasLocale: Option[String],
                       val queryUsingOriginalForm: Boolean,
                       val queryUsingBaseform: Boolean,
                       val queryUsingInflections: Seq[String],
                       val queryModifyingEveryPart: Boolean,
                       val queryModifyingOnlyLastPart: Boolean,
                       val queryUsingAllPermutations: Boolean,
                       val negativeLASFilters: Option[Map[String, Set[String]]],
                       val positiveLASFilters: Option[Map[String, Set[String]]],
                       val guess: Boolean,
                       val query: String,
                       val depth: Int,
                       val maxNGrams: Int) {
    def isSimple: Boolean = !queryUsingBaseform && queryUsingInflections.isEmpty && !negativeLASFilters.isDefined && !positiveLASFilters.isDefined

    def getConfiguration: JsValue = Json.obj(
      "name" -> name,
      "endpointURL" -> endpointURL,
      "lasLocale" -> lasLocale,
      "queryUsingOriginalForm" -> queryUsingOriginalForm,
      "queryUsingBaseform" -> queryUsingBaseform,
      "queryUsingInflections" -> queryUsingInflections,
      "queryModifyingEveryPart" -> queryModifyingEveryPart,
      "queryModifyingOnlyLastPart" -> queryModifyingOnlyLastPart,
      "queryUsingAllPermutations" -> queryUsingAllPermutations,
      "negativeLASFilters" -> negativeLASFilters,
      "positiveLASFilters" -> positiveLASFilters,
      "guess" -> guess,
      "query" -> query,
      "depth" -> depth,
      "maxNGrams" -> maxNGrams)
  }

  val serviceMap = new HashMap[String, Extractor]

  def buildExtractor(service: String, json: JsValue): Extractor = Extractor(service,
    (json \ "name").asOpt[String].getOrElse(throw new Exception("name")),
    (json \ "endpointURL").asOpt[String].getOrElse(throw new Exception("endpointURL")),
    (json \ "lasLocale").asOpt[String].flatMap(v => if (v.isEmpty) None else Some(v)),
    (json \ "queryUsingOriginalForm").asOpt[Boolean].getOrElse(false),
    (json \ "queryUsingBaseform").asOpt[Boolean].getOrElse(false),
    (json \ "queryUsingInflections").asOpt[Seq[String]].getOrElse(Seq.empty).filter(!_.isEmpty),
    (json \ "queryModifyingEveryPart").asOpt[Boolean].getOrElse(false),
    (json \ "queryModifyingOnlyLastPart").asOpt[Boolean].getOrElse(false),
    (json \ "queryUsingAllPermutations").asOpt[Boolean].getOrElse(false),
    (json \ "negativeLASFilters").asOpt[Map[String, Set[String]]],
    (json \ "positiveLASFilters").asOpt[Map[String, Set[String]]],
    (json \ "guess").asOpt[Boolean].getOrElse(false),
    (json \ "query").asOpt[String].getOrElse(throw new Exception("query")),
    (json \ "depth").asOpt[Int].getOrElse(1),
    (json \ "maxNGrams").asOpt[Int].getOrElse(throw new Exception("maxNGrams")))

  val servicesDir = new File(new SystemProperties().getOrElse("service.directory", "services"))
  servicesDir.mkdir()

  for (file <- servicesDir.listFiles()) {
    serviceMap(file.getName()) = buildExtractor(file.getName(), Json.parse(Source.fromFile(file).getLines.mkString))
  }
  
  def dispatch(service: String, text: Option[String], query: Option[String], locale: Option[String], pretty : Option[String], debug : Option[String], cgen : Option[String]) = {
    if (text.isDefined) extract(service, text, query, locale, pretty, debug, cgen)
    else configuration(service)
  }

  case class Analysis(val original: String, val whitespace : String,var completelyBaseformed: Option[String] = None,
    var lastPartBaseformed: Option[String] = None,
    var completelyInflected: Option[String] = None,
    var lastPartInflected: Option[String] = None,
    var allowed: Boolean = true
  )

  def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  def extract(service: String, text: Option[String], query: Option[String], locale: Option[String], pretty : Option[String], debug : Option[String], onlyGenerateCandidates: Option[String]) = Action.async { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) Future.successful(NotFound("Service " + service + " doesn't exist"))
    else {
      val service3 = service2.get
      val formBody = request.body.asFormUrlEncoded;
      var query2 = query
      var locale2 = locale
      var text2 = text
      formBody.foreach { data =>
        query2 = data.get("query").map(_(0)).orElse(query)
        locale2 = data.get("locale").map(_(0)).orElse(locale)
        text2 = data.get("text").map(_(0)).orElse(text)
      }
      var locale3 = locale.orElse(service3.lasLocale)
      var lm=0
      if (!text2.isDefined) Future.successful(BadRequest("No text given"))
      else if (text2.get.isEmpty) Future.successful(BadRequest("Empty text given"))
      else {
        val originalWordsPlusSeparators = (for (m <- "\\p{P}*(^|\\p{Z}+|$)\\p{P}*".r.findAllMatchIn(text2.get)) yield {
          val v = ((text2.get.substring(lm,m.start), m.matched))
          lm=m.end
          v
        }).filter(!_._1.isEmpty).toSeq
        var aresult : Option[JsValue] = None
        val originalWords = originalWordsPlusSeparators.map(_._1)
        val transformedWordsFuture = if (service3.isSimple && locale3.isDefined || originalWords.isEmpty) Future.successful(originalWordsPlusSeparators.map(w => new Analysis(w._1,w._2)))
        else analyzeWS.post(Map("text" -> Seq(originalWords.mkString(" ")), "guess" -> Seq(service3.guess.toString), "locale" -> locale3.toSeq, "forms" -> service3.queryUsingInflections, "depth" -> Seq(""+service3.depth))).flatMap { r1 =>
          val a = if (locale3.isDefined) r1.json
          else {
            locale3 = Some((r1.json \ "locale").as[String])
            r1.json \ "analysis"
          }
          aresult = Some(a)
          var wordsAndAnalyses = a.as[Seq[JsObject]].map { o =>
            var analysis = (o \ "analysis").as[Seq[JsObject]]
            var fanalysis = analysis.filter(o => (o \ "globalTags" \ "BEST_MATCH").as[Option[Seq[String]]].isDefined)
            ((o \ "word").as[String],(if (!fanalysis.isEmpty) fanalysis else analysis).apply(0))
          }.toMap
          val analyses = originalWordsPlusSeparators.map { originalWord =>
              val ret = new Analysis(originalWord._1,originalWord._2)
              if (wordsAndAnalyses.contains(originalWord._1)) {
                val wordAnalysis = wordsAndAnalyses(originalWord._1)
                if (service3.positiveLASFilters.isDefined || service3.negativeLASFilters.isDefined) {
                  val tags = (wordAnalysis \\ "tags").map(_.as[Map[String, Seq[String]]]).flatten.toMap
                  if (service3.negativeLASFilters.isDefined && !tags.forall {
                    case (key, vals) =>
                      val nfilters = service3.negativeLASFilters.get.getOrElse(key, Set.empty)
                      vals.forall(v => !nfilters.exists(v.startsWith(_)))
                  }) ret.allowed = false
                  if (service3.positiveLASFilters.isDefined && !service3.positiveLASFilters.get.forall {
                    case (key, vals) =>
                      tags.isDefinedAt(key) && vals.exists(v => tags(key).contains(v))
                  }) ret.allowed = false
                }
                val wordParts = (wordAnalysis \ "wordParts").as[Seq[JsObject]]
                if (service3.queryModifyingEveryPart)
                  ret.completelyBaseformed = Some(wordParts.map(o => (o \ "lemma").as[String]).mkString)
                if (service3.queryModifyingOnlyLastPart) {
                  val segments = wordParts.dropRight(1).map(o => {
                    val segments = (o \\ "SEGMENT")
                    if (!segments.isEmpty)
                      segments.map(_.as[Seq[String]]).flatten.filter(_ != "-0").map(_.replaceAllLiterally("»", "").replaceAllLiterally("{WB}","").replaceAllLiterally("{XB}","").replaceAllLiterally("{DB}","").replaceAllLiterally("{MB}","").replaceAllLiterally("{STUB}","").replaceAllLiterally("{hyph?}","")).mkString
                      else (o \ "lemma").as[String] 
                  })
                  ret.lastPartBaseformed = Some((segments :+ ((wordParts.last \ "lemma").as[String])).mkString)
                }
                if (service3.queryModifyingEveryPart)
                  ret.completelyInflected = Some(wordParts.map{o =>
                      val inf = (o \\ "INFLECTED").map(_.as[Seq[String]]).flatten
                      if (!inf.isEmpty) inf(0) else (o \ "lemma").as[String]}.mkString)
                if (service3.queryModifyingOnlyLastPart) {
                  val segments = wordParts.dropRight(1).map(o => {
                    val segments = (o \\ "SEGMENT")
                    if (!segments.isEmpty)
                      segments.map(_.as[Seq[String]]).flatten.filter(_ != "-0").map(_.replaceAllLiterally("»", "").replaceAllLiterally("{WB}","").replaceAllLiterally("{XB}","").replaceAllLiterally("{DB}","").replaceAllLiterally("{MB}","").replaceAllLiterally("{STUB}","").replaceAllLiterally("{hyph?}","")).mkString
                      else (o \ "lemma").as[String] 
                  })
                  ret.lastPartInflected = Some((segments :+ ({
                    val inf = (wordParts.last \\ "INFLECTED").map(_.as[Seq[String]]).flatten
                    if (!inf.isEmpty) inf(0) else (wordParts.last \ "lemma").as[String]
                  })).mkString)
                }
              } else {
                if (service3.queryModifyingEveryPart) {
                  ret.completelyBaseformed = Some(originalWord._1)
                  ret.completelyInflected = Some(originalWord._1)
                }
                if (service3.queryModifyingOnlyLastPart) {
                  ret.lastPartBaseformed = Some(originalWord._1)
                  ret.lastPartInflected = Some(originalWord._1)
                }
              }
              ret
          }
          Future.successful(analyses)
        }
        transformedWordsFuture.map { words =>
          val ngrams = new HashSet[String]
          val ngramOriginalMap = new HashMap[String, HashSet[String]]
          var lastInflected: Seq[String] = Seq.empty
          var lastBaseformed: Seq[String] = Seq.empty
          var lastOriginal: Seq[String] = Seq.empty
          var lastAllowed: Seq[Boolean] = Seq.empty
          for (word <- words) {
            lastAllowed = (lastAllowed :+ word.allowed).takeRight(service3.maxNGrams)
            if (word.allowed) {
              var t = (lastOriginal :+ word.original).takeRight(service3.maxNGrams)
              for (i <- 1 to t.length) if (lastAllowed.takeRight(i).forall(b=>b)) {
                if (service3.queryUsingOriginalForm) {
                  val ngram = t.takeRight(i).mkString("")
                  ngrams += FmtUtils.stringForString(ngram)
                  ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += ngram
                }
                word.lastPartBaseformed.foreach { w =>
                  val ngram = (lastOriginal :+ w).takeRight(i).mkString("")
                  ngrams += FmtUtils.stringForString(ngram)
                  ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
                }
                word.lastPartInflected.foreach { w =>
                  val ngram = (lastOriginal :+ w).takeRight(i).mkString("")
                  ngrams += FmtUtils.stringForString(ngram)
                  ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
                }
              }
              word.completelyBaseformed.foreach { word =>
                var t2 = (lastBaseformed :+ word).takeRight(service3.maxNGrams)
                for (head <- if (service3.queryUsingAllPermutations) combine(t,t2) else Seq(t2))
                  for (i <- 1 to head.length) if (lastAllowed.takeRight(i).forall(b=>b)) {
                    val ngram = head.takeRight(i).mkString("")
                    ngrams += FmtUtils.stringForString(ngram)
                    ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
                  }
              }
              word.completelyInflected.foreach { word =>
                var t2 = (lastInflected :+ word).takeRight(service3.maxNGrams)
                for (head <- if (service3.queryUsingAllPermutations) combine(t,t2) else Seq(t2))
                  for (i <- 1 to head.length) if (lastAllowed.takeRight(i).forall(b=>b)) {
                    val ngram = head.takeRight(i).mkString("")
                    ngrams += FmtUtils.stringForString(ngram)
                    ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
                  }
              }
            }
            lastOriginal = (lastOriginal :+ (word.original+word.whitespace)).takeRight(service3.maxNGrams)
            if (word.completelyBaseformed.isDefined) lastBaseformed = (lastBaseformed :+ (word.completelyBaseformed.get+word.whitespace)).takeRight(service3.maxNGrams)
            if (word.completelyInflected.isDefined) lastInflected = (lastInflected :+ (word.completelyInflected.get+word.whitespace)).takeRight(service3.maxNGrams)
          }
          if (onlyGenerateCandidates.isDefined && (onlyGenerateCandidates.get=="" || onlyGenerateCandidates.get.toBoolean)) {
            if (pretty.isDefined && (pretty.get=="" || pretty.get.toBoolean))
              Ok(Json.prettyPrint(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ngramOriginalMap.toMap.map{case (k,v) => (k,v.toSeq)})))))
            else
              Ok(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ngramOriginalMap.toMap.map{case (k,v) => (k,v.toSeq)}))))
          } else {
            var queryString = service3.query.replaceAllLiterally("<VALUES>", ngrams.mkString(" ")).replaceAllLiterally("<LANG>",'"'+locale3.get+'"')
            query2.foreach(q => queryString = queryString.replaceAll("# QUERY", q))
            try {
              val resultSet = QueryExecutionFactory.sparqlService(service3.endpointURL, queryString).execSelect()
              val rmap = new HashMap[String, (Option[String],HashSet[String],HashMap[String,Buffer[String]])]
              while (resultSet.hasNext()) {
                val solution = resultSet.nextSolution
                val id = solution.getResource("id").getURI
                val (_,ngrams,properties) = rmap.getOrElseUpdate(id, (Option(solution.getLiteral("label")).map(_.getString),new HashSet[String],new HashMap[String,Buffer[String]]))
                for (v <- solution.varNames)
                  properties.getOrElseUpdate(v, new ArrayBuffer[String]) += FmtUtils.stringForRDFNode(solution.get(v))
                ngrams += solution.getLiteral("ngram").getString
              }
              val ret = for ((id, (label,ngrams,properties)) <- rmap) yield {
                val matches = new ArrayBuffer[String]
                for (
                  ngram <- ngrams;
                  ongram <- ngramOriginalMap(ngram)
                ) matches += ongram
                ExtractionResult(id, label, matches,properties.view.toMap.map{case (k,v) => (k,v.toSeq)})
              }
              if (debug.isDefined && (debug.get=="" || debug.get.toBoolean))
                Ok("ARPA output:\n"+aresult.map(Json.prettyPrint(_)).getOrElse("not run")+"\nQuery:\n"+queryString+"\nResults:\n"+Json.prettyPrint(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ret)))))
              else if (pretty.isDefined && (pretty.get=="" || pretty.get.toBoolean))
                Ok(Json.prettyPrint(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ret)))))
              else
                Ok(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ret))))
            } catch {
              case e: QueryParseException => InternalServerError(e.getMessage + " parsing query:\n" + queryString)
              case e: Exception => InternalServerError(e.getMessage + " for query:\n" + queryString)
            }
          }
        }
      }
    }.recover {
      case e: Exception => e.printStackTrace(); InternalServerError("Encountered exception: "+e.toString)
    }
  }

  case class ExtractionResult(
    var id: String,
    var label: Option[String],
    var matches: Seq[String],
    var properties: Map[String,Seq[String]])
  implicit val ExtractionResultWrites = new Writes[ExtractionResult] {
    def writes(r: ExtractionResult): JsValue = {
      Json.obj(
        "id" -> r.id,
        "label" -> r.label,
        "matches" -> r.matches,
        "properties" -> r.properties)
    }
  }
  
  val AcceptsHJson = Accepting("text/hjson")

  def configuration(service: String) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service " + service + " doesn't exist")
    else {
     val config = service2.get.getConfiguration
     render {
       case Accepts.Json() => Ok(Json.prettyPrint(config))
       case AcceptsHJson() => Ok(JsonValue.readJSON(config.toString()).toString(Stringify.HJSON)) 
       case _ => Ok(Json.prettyPrint(config))
     }
    }
  }

  def services() = Action {
    Ok(Json.prettyPrint(Json.toJson(serviceMap.map { case (k, v) => (k, v.name) }.toMap)))
  }

  def configure(service: String) = Action(parse.tolerantText) { request =>
    try {
      val json = Json.parse(JsonValue.readHjson(request.body).toString())
      serviceMap(service) = buildExtractor(service, json)
      val fw = new FileWriter("services/" + service)
      fw.write(Json.prettyPrint(json))
      fw.close()
      Ok("Service " + service + " (" + (json \ "name") + ") saved.\n")
    } catch {
      case e: Exception => BadRequest("Badly formatted request: " + e.toString())
    }
  }

  def delete(service: String) = Action {
    import play.api.Play.current
    import play.api.libs.ws._
    import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder
    import scala.concurrent.Future
    val service2 = serviceMap.remove(service)
    if (!service2.isDefined) NotFound("Service " + service + " doesn't exist")
    else {
      new File(servicesDir.getAbsolutePath + "/" + service).delete()
      Ok("Service " + service + " (" + service2.get.name + ") deleted.\n")
    }
  }

  def preflight(all: String) = Action {
    Ok("").withHeaders("Access-Control-Allow-Origin" -> "*",
      "Allow" -> "*",
      "Access-Control-Allow-Methods" -> "POST, GET, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, Accept, Referrer, User-Agent",
      "Access-Control-Max-Age" -> "3600",
      "Access-Control-Allow-Credentials" -> "true")
  }

}
