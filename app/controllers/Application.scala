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
                       val query: String,
                       val maxNGrams: Int) {
    def isSimple: Boolean = !queryUsingBaseform && queryUsingInflections.isEmpty && !negativeLASFilters.isDefined && !positiveLASFilters.isDefined

    def writeConfiguration: JsValue = Json.obj(
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
      "query" -> query,
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
    (json \ "lasFilters").asOpt[Map[String, Set[String]]].map(m => m.map { case (k, v) => (k, v.filter(_.startsWith("!")).map(_.substring(1))) }.filter(!_._2.isEmpty)),
    (json \ "lasFilters").asOpt[Map[String, Set[String]]].map(m => m.map { case (k, v) => (k, v.filter(!_.startsWith("!"))) }.filter(!_._2.isEmpty)),
    (json \ "query").asOpt[String].getOrElse(throw new Exception("query")),
    (json \ "maxNGrams").asOpt[Int].getOrElse(throw new Exception("maxNGrams")))

  val servicesDir = new File(new SystemProperties().getOrElse("service.directory", "services"))
  servicesDir.mkdir()

  for (file <- servicesDir.listFiles()) {
    serviceMap(file.getName()) = buildExtractor(file.getName(), Json.parse(Source.fromFile(file).getLines.mkString))
  }

  def dispatch(service: String, text: Option[String], query: Option[String], locale: Option[String]) = {
    if (text.isDefined) extract(service, text, query, locale)
    else configuration(service)
  }

  case class Analysis(val original: String, val whitespace : String,var completelyBaseformed: Option[String] = None,
    var lastPartBaseformed: Option[String] = None,
    var completelyInflected: Option[String] = None,
    var lastPartInflected: Option[String] = None
  )

  def combine[A](a: Seq[A],b: Seq[A]): Seq[Seq[A]] =
    a.zip(b).foldLeft(Seq.empty[Seq[A]]) { (x,s) => if (x.isEmpty) Seq(Seq(s._1),Seq(s._2)) else (for (a<-x) yield Seq(a:+s._1,a:+s._2)).flatten }

  def extract(service: String, text: Option[String], query: Option[String], locale: Option[String]) = Action.async { implicit request =>
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
      val originalWordsPlusSeparators = (for (m <- "\\p{P}*(^|\\p{Z}+|$)\\p{P}*".r.findAllMatchIn(text2.get)) yield {
        val v = ((text2.get.substring(lm,m.start), m.matched))
        lm=m.end
        v
      }).filter(!_._1.isEmpty).toSeq
      val originalWords = originalWordsPlusSeparators.map(_._1)
      val transformedWordsFuture = if (service3.isSimple && locale3.isDefined) Future.successful(originalWordsPlusSeparators.map(w => new Analysis(w._1,w._2)))
      else analyzeWS.post(Map("text" -> Seq(originalWords.toSet.mkString(" ")), "locale" -> locale3.toSeq, "forms" -> service3.queryUsingInflections, "depth" -> Seq("0"))).flatMap { r1 =>
        val a = if (locale3.isDefined) r1.json
        else {
          locale3 = Some((r1.json \ "locale").as[String])
          r1.json \ "analysis"
        }
        var wordsAndAnalyses = a.as[Seq[JsObject]].map { o => ((o \ "word").as[String],((o \ "analysis").as[Seq[JsObject]].apply(0))) }.toMap
        if (service3.positiveLASFilters.isDefined || service3.negativeLASFilters.isDefined)
          wordsAndAnalyses = wordsAndAnalyses.filter {
            case (originalWord, wordAnalysis) =>
              val tags = (wordAnalysis \\ "tags").map(_.as[Map[String, Seq[String]]]).flatten.toMap
              tags.forall {
                case (key, vals) =>
                  val pfilters = service3.positiveLASFilters.get.getOrElse(key, Set.empty)
                  val nfilters = service3.negativeLASFilters.get.getOrElse(key, Set.empty)
                  vals.exists(v => pfilters.isEmpty || pfilters.exists(v.startsWith(_))) && vals.forall(v => !nfilters.exists(v.startsWith(_)))
              }
          }
        val analyses = originalWordsPlusSeparators.map { originalWord =>

            val ret = new Analysis(originalWord._1,originalWord._2)
            if (wordsAndAnalyses.contains(originalWord._1)) {
              val wordAnalysis = wordsAndAnalyses(originalWord._1)
              val wordParts = (wordAnalysis \ "wordParts").as[Seq[JsObject]]
              if (service3.queryModifyingEveryPart)
                ret.completelyBaseformed = Some(wordParts.map(o => (o \ "lemma").as[String]).mkString)
              if (service3.queryModifyingOnlyLastPart)
                ret.lastPartBaseformed = Some((wordParts.dropRight(1).map(o => (o \\ "SEGMENT").map(_.as[Seq[String]]).flatten.filter(_ != "-0").map(_.replaceAllLiterally("»", "").replaceAllLiterally("{WB}","").replaceAllLiterally("{XB}","").replaceAllLiterally("{DB}","").replaceAllLiterally("{MB}","").replaceAllLiterally("{STUB}","").replaceAllLiterally("{hyph?}","")).mkString) :+ ((wordParts.last \ "lemma").as[String])).mkString)
              if (service3.queryModifyingEveryPart)
                ret.completelyInflected = Some(wordParts.map{o =>
                    val inf = (o \\ "INFLECTED").map(_.as[Seq[String]]).flatten
                    if (!inf.isEmpty) inf(0) else (o \ "lemma").as[String]}.mkString)
              if (service3.queryModifyingOnlyLastPart)
                ret.lastPartInflected = Some((wordParts.dropRight(1).map(o => (o \\ "SEGMENT").map(_.as[Seq[String]]).flatten.filter(_ != "-0").map(_.replaceAllLiterally("»", "").replaceAllLiterally("{WB}","").replaceAllLiterally("{XB}","").replaceAllLiterally("{DB}","").replaceAllLiterally("{MB}","").replaceAllLiterally("{STUB}","").replaceAllLiterally("{hyph?}","")).mkString) :+ ({
                  val inf = (wordParts.last \\ "INFLECTED").map(_.as[Seq[String]]).flatten
                  if (!inf.isEmpty) inf(0) else (wordParts.last \ "lemma").as[String]
                })).mkString)
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
        for (word <- words) {
          var t = (lastOriginal :+ word.original).takeRight(service3.maxNGrams)
          for (i <- 1 to t.length) {
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
              for (i <- 1 to head.length) {
                val ngram = head.takeRight(i).mkString("")
                ngrams += FmtUtils.stringForString(ngram)
                ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
              }
          }
          word.completelyInflected.foreach { word =>
            var t2 = (lastInflected :+ word).takeRight(service3.maxNGrams)
            for (head <- if (service3.queryUsingAllPermutations) combine(t,t2) else Seq(t2))
              for (i <- 1 to head.length) {
                val ngram = head.takeRight(i).mkString("")
                ngrams += FmtUtils.stringForString(ngram)
                ngramOriginalMap.getOrElseUpdate(ngram, new HashSet[String]) += t.takeRight(i).mkString("")
              }
          }
          lastOriginal = (lastOriginal :+ (word.original+word.whitespace)).takeRight(service3.maxNGrams)
          if (word.completelyBaseformed.isDefined) lastBaseformed = (lastBaseformed :+ (word.completelyBaseformed.get+word.whitespace)).takeRight(service3.maxNGrams)
          if (word.completelyInflected.isDefined) lastInflected = (lastInflected :+ (word.completelyInflected.get+word.whitespace)).takeRight(service3.maxNGrams)
        }
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
          Ok(Json.toJson(Map("locale"->JsString(locale3.get),"results"->Json.toJson(ret))))
        } catch {
          case e: QueryParseException => InternalServerError(e.getMessage + " parsing " + queryString)
        }
      }
    }.recover {
      case e: Exception => InternalServerError(e.getMessage)
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

  def configuration(service: String) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service " + service + " doesn't exist")
    else Ok(service2.get.writeConfiguration)
  }

  def services() = Action {
    Ok(Json.toJson(serviceMap.map { case (k, v) => (k, v.name) }.toMap))
  }

  def configure(service: String) = Action { implicit request =>
    val jsonBody = request.body.asJson
    if (!jsonBody.isDefined) BadRequest("Expecting a JSON body\n")
    else try {
      val json = jsonBody.get
      serviceMap(service) = buildExtractor(service, json)
      val fw = new FileWriter("services/" + service)
      fw.write(json.toString())
      fw.close()
      Ok("Service " + service + " (" + (json \ "name") + ") saved.\n")
    } catch {
      case e: Exception => BadRequest("Badly formatted request: missing " + e.getMessage)
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
