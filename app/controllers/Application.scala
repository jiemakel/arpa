package controllers

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

object Application extends Controller {

  val servicePrefix = new SystemProperties().getOrElse("service.prefix","http://localhost:9000/")
  
  case class SPARQLReconcileEndpoint(id:String,
    val name : String,
    val endpointURL : String,
    val reconcileQuery : String,
    val suggestTypeQuery : String,
    val suggestPropertyQuery : String,
    val suggestEntityQuery : String,
    val typeFilter : String,
    val propertyFilter : String,
  
    val entityViewURL : String,
  
    val entityPreviewURL : String,
    val entityPreviewHeight : Int,
    val entityPreviewWidth : Int,
  
    val typeFlyoutURL : String,
    val typeFlyoutHeight : Int,
    val typeFlyoutWidth : Int,
    
    val propertyFlyoutURL : String,
    val propertyFlyoutHeight : Int,
    val propertyFlyoutWidth : Int,
    
    val entityFlyoutURL : String,
    val entityFlyoutHeight : Int,
    val entityFlyoutWidth : Int,
    
    val matchThreshold : Double) {
         
    val serviceURL = servicePrefix+id
  
    def writeMetadata() : JsValue = Json.obj(
        "name" -> name,
        "schemaSpace" -> "http://www.ietf.org/rfc/rfc3986",
        "identifierSpace" -> "http://www.ietf.org/rfc/rfc3986",
        "view" -> Json.toJson(Map("url"->entityViewURL)),
        "preview" -> Json.toJson(Map("url"->JsString(entityPreviewURL),"height"->JsNumber(entityPreviewHeight),"width"->JsNumber(entityPreviewWidth))),
        "suggest" -> Json.toJson(Map(
            "type"->Json.toJson(Map(
                "service_url"->serviceURL,
                "service_path"->"/suggest/type",
                "flyout_service_url"->serviceURL,
                "flyout_service_path"->"/suggest/type/flyout?url=${id}")),
            "property"->Json.toJson(Map(
                "service_url"->serviceURL,
                "service_path"->"/suggest/property",
                "flyout_service_url"->serviceURL,
                "flyout_service_path"->"/suggest/property/flyout?url=${id}")),
            "entity"->Json.toJson(Map(
                "service_url"->serviceURL,
                "service_path"->"/suggest/entity",
                "flyout_service_url"->serviceURL,
                "flyout_service_path"->"/suggest/entity/flyout?url=${id}"))
         ))
      )
    
    def writeConfiguration() : JsValue = Json.obj(
        "name" -> name,
        "endpointURL" -> endpointURL,
        "reconcileQuery" -> reconcileQuery,
        "suggestTypeQuery" -> suggestTypeQuery,
        "suggestPropertyQuery" -> suggestPropertyQuery,
        "suggestEntityQuery" -> suggestEntityQuery,
        "typeFilter" -> typeFilter,
        "propertyFilter" -> propertyFilter,
        "entityViewURL" -> entityViewURL,
        "entityPreviewURL" -> entityPreviewURL,
        "entityPreviewHeight" -> entityPreviewHeight,
        "entityPreviewWidth" -> entityPreviewWidth,
        "typeFlyoutURL" -> typeFlyoutURL,
        "propertyFlyoutURL" -> propertyFlyoutURL,
        "entityFlyoutURL" -> entityFlyoutURL,
        "matchThreshold" -> matchThreshold)    
  }

  val serviceMap = new HashMap[String,SPARQLReconcileEndpoint]

  def buildSPARQLReconcileEndpoint(service:String,json: JsValue) : SPARQLReconcileEndpoint = SPARQLReconcileEndpoint(service,
    (json \ "name").asOpt[String].getOrElse(throw new Exception("name")),
    (json \ "endpointURL").asOpt[String].getOrElse(throw new Exception("endpointURL")),
    (json \ "reconcileQuery").asOpt[String].getOrElse(throw new Exception("reconcileQuery")),
    (json \ "suggestTypeQuery").asOpt[String].getOrElse(throw new Exception("suggestTypeQuery")),
    (json \ "suggestPropertyQuery").asOpt[String].getOrElse(throw new Exception("suggestPropertyQuery")),
    (json \ "suggestEntityQuery").asOpt[String].getOrElse(throw new Exception("suggestEntityQuery")),
    (json \ "typeFilter").asOpt[String].getOrElse(throw new Exception("typeFilter")),
    (json \ "propertyFilter").asOpt[String].getOrElse(throw new Exception("propertyFilter")),
    (json \ "entityViewURL").asOpt[String].getOrElse(throw new Exception("entityViewURL")),
    (json \ "entityPreviewURL").asOpt[String].getOrElse(throw new Exception("entityPreviewURL")),
    (json \ "entityPreviewHeight").asOpt[Int].getOrElse(throw new Exception("entityPreviewHeight")),
    (json \ "entityPreviewWidth").asOpt[Int].getOrElse(throw new Exception("entityPreviewWidth")),
    (json \ "typeFlyoutURL").asOpt[String].getOrElse(throw new Exception("typeFlyoutURL")),
    (json \ "typeFlyoutHeight").asOpt[Int].getOrElse(throw new Exception("typeFlyoutHeight")),
    (json \ "typeFlyoutWidth").asOpt[Int].getOrElse(throw new Exception("typeFlyoutWidth")),
    (json \ "propertyFlyoutURL").asOpt[String].getOrElse(throw new Exception("propertyFlyoutURL")),
    (json \ "propertyFlyoutHeight").asOpt[Int].getOrElse(throw new Exception("propertyFlyoutHeight")),
    (json \ "propertyFlyoutWidth").asOpt[Int].getOrElse(throw new Exception("propertyFlyoutWidth")),
    (json \ "entityFlyoutURL").asOpt[String].getOrElse(throw new Exception("entityFlyoutURL")),
    (json \ "entityFlyoutHeight").asOpt[Int].getOrElse(throw new Exception("entityFlyoutHeight")),
    (json \ "entityFlyoutWidth").asOpt[Int].getOrElse(throw new Exception("entityFlyoutWidth")),
    (json \ "matchThreshold").asOpt[Double].getOrElse(throw new Exception("matchThreshold")))

  val servicesDir = new File(new SystemProperties().getOrElse("service.directory","services"))
  servicesDir.mkdir()
    
  for(file <- servicesDir.listFiles()) {
   serviceMap(file.getName())=buildSPARQLReconcileEndpoint(file.getName(),Json.parse(Source.fromFile(file).getLines.mkString)) 
  }

  def reconcile(service:String,query:Option[String],queries:Option[String],callback : Option[String]) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service "+service+" doesn't exist")
    else {
      implicit val service3 = service2.get
      val formBody = request.body.asFormUrlEncoded;
      var query2 = query
      var queries2 = queries
      var callback2 = callback
      formBody.foreach { data =>
        query2 = data.get("query").map(_(0)).orElse(query)
        queries2 = data.get("queries").map(_(0)).orElse(queries)
        callback2 = data.get("callback").map(_(0)).orElse(callback)
      }
      implicit val callback3 = callback2
      val query3 = try {
        query2.map(Json.parse(_))
      } catch {
        case e : Exception => query2.map(x => Json.obj("query"->x))
      }
      val queries3 = queries2.map(Json.parse(_))
      if (query3.isDefined) reconcileSingle(query3.get)
      else if (queries3.isDefined) reconcileMultiple(queries3.get.as[JsObject])
      else metadata()
    }
  }

  implicit def toResponse(res : Either[JsValue, String])(implicit request : Request[AnyContent], callback: Option[String]) : Result = {
    res match {
        case Left(y) => Ok(callback.map { _ + "("+y.toString+")" }.getOrElse(y.toString()))
        case Right(y) => BadRequest(y)
    }
  }

  def reconcileSingle(query:JsValue)(implicit service : SPARQLReconcileEndpoint) : Either[JsValue,String] = {
    reconcileMultiple(Json.obj("q0"->query)).left.map { _ \ "q0" }
  }
  
  def combine(xs: Seq[JsObject]): Seq[Seq[JsObject]] = xs.foldLeft(Seq(Seq.empty[JsObject])){ (x, y) => 
    var seq = (y \ "v").asOpt[Seq[JsObject]].getOrElse(Seq.empty)
    for (a <-x; b <- seq) yield a :+ Json.obj("name"->(y \ "name").as[String],"pid"->(y \ "pid").as[String],"v"-> Seq(b))
  }
  
  class ReconcileResult(
    var id : String,
    var name : String,
    var types : Seq[String] = Seq.empty,
    var score : Double = 1.0,
    var matches : Boolean = false
  )
  implicit val ReconcileResultWrites = new Writes[ReconcileResult] {
    def writes(r : ReconcileResult) : JsValue = {
      Json.obj(
        "id" -> r.id,
        "name" -> r.name,
        "type" -> r.types,
        "score" -> r.score,
        "match" -> r.matches
      )
    }
  }
 
  def levenshteinSubstring(needle: String, haystack:String): Double = {
    var row1 = new Array[Int](haystack.length+1)
    var row2 = new Array[Int](haystack.length+1)
    for (i <- 0 until needle.length) {
      row2(0) = i+1
      for (j <- 0 until haystack.length)
        row2(j+1) = Seq(row1(j+1)+1,row2(j)+1,row1(j)+(if (needle(i)==haystack(j)) 0 else 1)).min
      var tmp = row2
      row2=row1
      row1=tmp
    }
    1.0-(row1.min.toDouble/needle.length)
  }  
  
  def reconcileMultiple(queries:JsObject)(implicit service : SPARQLReconcileEndpoint) : Either[JsValue,String] = {
    val queryParts = p.split(service.reconcileQuery)
    val queryRep = p.findFirstMatchIn(service.reconcileQuery).get.group(1)
    val queryTerms = new HashMap[String,String]
    var queryString = queryParts(0)
    for ((qid,query)<-queries.fieldSet) {
      val limit = (query \ "limit").asOpt[Int]
      val typesOpt = (query \ "type").asOpt[Seq[String]].orElse((query \ "type").asOpt[String].map(Seq(_)))
      val mode = (query \ "type_strict").asOpt[String].getOrElse("any")
      val queryTerm = (query \ "query").as[String]
      queryTerms.put(qid,queryTerm)
      var qp = queryRep.replace("<QUERY_ID>",FmtUtils.stringForString(qid)).replace("<QUERY>",FmtUtils.stringForString(queryTerm))
      var propertyFilters = ""
      for (properties <- (query \ "properties").asOpt[Seq[JsObject]];property<-properties) {
        var vv = (property \ "v").as[JsValue]
        val vs = if (vv.isInstanceOf[JsArray]) vv.as[Seq[JsValue]] else Seq(vv)
        var pf=service.propertyFilter.replace("<PROPERTY_NAME>",FmtUtils.stringForString((property \ "name").as[String]))
        pf = pf.replace("<PROPERTY>",FmtUtils.stringForURI((property \ "pid").as[String]))        
        if (vs.length>1) propertyFilters+="{"
        for (v <- vs) {
          if (v.isInstanceOf[JsString]) propertyFilters += pf.replace("<VALUE>",FmtUtils.stringForString(v.as[String]))
          else if (v.isInstanceOf[JsObject]) propertyFilters += pf.replace("<VALUE>",FmtUtils.stringForURI((v \ "id").as[String]))
          else propertyFilters += pf.replace("<VALUE>",(v \ "id").as[String])
          propertyFilters += "}UNION{"
        }
        if (vs.length==1) propertyFilters=propertyFilters.substring(0,propertyFilters.length-7)
        else if (vs.length>0) propertyFilters=propertyFilters.substring(0,propertyFilters.length-6)
      }
      qp=qp.replace("# PROPERTY_FILTERS",propertyFilters)
      var typesFilter = ""
      if (mode=="any" || mode=="should") {
        typesOpt.foreach { types => 
          if (types.length>1) typesFilter+="{"
          for (t<-types) {
            typesFilter += service.typeFilter.replace("<TYPE>",FmtUtils.stringForURI(t)) 
            typesFilter += "}UNION{"
          }
          if (types.length==1) typesFilter=typesFilter.substring(0,typesFilter.length-7)
          else if (types.length>0) typesFilter=typesFilter.substring(0,typesFilter.length-6)
        }
      } else for (types <- typesOpt;t<-types) typesFilter += service.typeFilter.replace("<TYPE>",FmtUtils.stringForURI(t))
      qp=qp.replace("# TYPE_FILTER",typesFilter)
      qp=qp.replace("LIMIT 0",limit.map("LIMIT "+_).getOrElse(""))
      queryString+="{"+qp+"}UNION"
    }
    queryString=queryString.substring(0,queryString.length-5)+queryParts(1)
    try {
      val resultSet = QueryExecutionFactory.sparqlService(service.endpointURL, queryString).execSelect()
      val seenMap = new HashMap[String,HashSet[String]]
      val resultMap = new HashMap[String,HashMap[String,ReconcileResult]]
      val typesMap = new HashMap[String,HashSet[String]]
      while(resultSet.hasNext()) {
        val solution = resultSet.nextSolution
        val qid = solution.getLiteral("queryId").getString
        val seenOpt = seenMap.get(qid)
        var seen : HashSet[String] = null
        var results : HashMap[String,ReconcileResult] = null
        if (!seenOpt.isDefined) {
          seen = new HashSet[String]
          seenMap.put(qid,seen)
          results = new HashMap[String,ReconcileResult]
          resultMap.put(qid,results)
        } else {
          seen=seenOpt.get
          results = resultMap(qid)
        }
        val entity = if (solution.get("entity").isLiteral) solution.getLiteral("entity").getString else solution.getResource("entity").getURI
        if (solution.contains("type")) {
            val typesOpt = typesMap.get(entity)
            var types : HashSet[String] = null
            if (!typesOpt.isDefined) {
                types = new HashSet[String]()
                typesMap.put(entity,types)
            } else types = typesOpt.get
            types.add(solution.getResource("type").getURI());
        }
        val label = solution.getLiteral("label").getString
        val curResult = results.get(entity)
        val score = if (solution.contains("score")) solution.getLiteral("score").getDouble else levenshteinSubstring(queryTerms(qid),label)
        if (!curResult.isDefined) results.put(entity,new ReconcileResult(entity,label,null,score,score>service.matchThreshold))
        else curResult.foreach { x => if (x.score<score) {
          x.name=label
          x.score=score
          x.matches=score>service.matchThreshold
        }}          
      }
      for ((qid,queryTerm) <- queryTerms) resultMap.getOrElseUpdate(qid, new HashMap[String,ReconcileResult])
      Left(Json.toJson(resultMap.toMap.map { case (qid,results) => 
        var matches = 0
        for ((id,result) <- results) {
          if (result.matches) matches+=1
          result.types=typesMap.get(id).map(_.toSeq).getOrElse(Seq.empty)
        }
        if (matches>1) for ((id,result) <- results) result.matches=false
        (qid,Json.obj("result"->results.values.toSeq.sortBy { -_.score }))
      }))
    } catch {
      case e : QueryParseException => throw new IllegalArgumentException(e.getMessage+" parsing "+queryString,e)
    }
  }
  
  val p = "(?s)\\{ # QUERY(.*?)\\} # /QUERY".r
  
  def suggest(service:String,prefix:Option[String],type_strict:String,limit:Option[Int],start:Int,callback:Option[String],getQuery : (SPARQLReconcileEndpoint)=>String) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service "+service+" doesn't exist")
    else {
      implicit val service3 = service2.get
      val formBody = request.body.asFormUrlEncoded;
      var prefix2 = prefix
      val typesOpt = request.queryString.get("type").filter { x => x!=Seq("/type/property") }
      var type_strict2 = type_strict
      var limit2 = limit
      var start2 = start
      var callback2 = callback
      formBody.foreach { data =>
        prefix2 = data.get("prefix").map(_(0)).orElse(prefix)
        type_strict2 = data.get("type_strict").map(_(0)).getOrElse(type_strict)
        limit2 = data.get("limit").map(_(0).toInt).orElse(limit)
        start2 = data.get("start").map(_(0).toInt).getOrElse(start)
        callback2 = data.get("callback").map(_(0)).orElse(callback)
      }
      if (!prefix2.isDefined) BadRequest("prefix not defined")
      else {
        var query = getQuery(service3).replace("<QUERY>",FmtUtils.stringForString(prefix2.get))
        var typesFilter = ""
        if (type_strict=="any" || type_strict=="should") {
          typesOpt.foreach { types => 
            if (types.length>1) typesFilter+="{"
            for (t<-types) {
              typesFilter += service3.typeFilter.replace("<TYPE>",FmtUtils.stringForURI(t)) 
              typesFilter += "}UNION{"
            }
            if (types.length==1) typesFilter=typesFilter.substring(0,typesFilter.length-7)
            else if (types.length>0) typesFilter=typesFilter.substring(0,typesFilter.length-6)
          }
        } else for (types <- typesOpt;t<-types) typesFilter += service3.typeFilter.replace("<TYPE>",FmtUtils.stringForURI(t))
        query=query.replace("# TYPE_FILTER",typesFilter)
        try {
          val resultSet = QueryExecutionFactory.sparqlService(service2.get.endpointURL,query).execSelect()
          val results = new HashMap[String,String]
          val typeMap = new HashMap[String,String]
          val typeLabelMap = new HashMap[String,String]
          while(resultSet.hasNext()) {
            val sol = resultSet.nextSolution
            val value = sol.getResource("value").getURI
            val label = sol.getLiteral("label").getString
            if (sol.contains("type")) {
              val atype = sol.getResource("type").getURI
              typeMap.put(value,atype)
              if (sol.contains("typeLabel")) {
                typeLabelMap.put(atype,sol.getLiteral("typeLabel").getString)
              }
            }
            val co = results.get(value)
            if (co.isDefined) {
              val clabel = co.get
              if (label.length<clabel.length) results.put(value,label)
            } else results.put(value,label)
          }
          val r = Json.obj("code" -> "/api/status/ok","status" -> "200 OK","prefix"->prefix,"result"->results.map{ case (t,l) => 
            val typeOpt = typeMap.get(t)
            if (typeOpt.isDefined) {
              val atype = typeOpt.get
              val typeNameOpt = typeLabelMap.get(atype)
              if (typeNameOpt.isDefined) Json.obj("id"->t,"name"->l,"n:type"->Json.obj("id"->atype,"name"->typeNameOpt.get))
              else Json.obj("id"->t,"name"->l,"n:type"->Json.obj("id"->atype))
            } else Json.obj("id"->t,"name"->l) 
          })
          Ok(callback2.map { _ + "("+r.toString+")" }.getOrElse(r.toString()))
        } catch {
          case e : QueryParseException => throw new IllegalArgumentException(e.getMessage+" parsing "+query,e)
        }
      }
    }
  }
  
  def suggestType(service:String,prefix:Option[String],type_strict:String,limit:Option[Int],start:Int,callback:Option[String]) = suggest(service,prefix,type_strict,limit,start,callback,(x)=>x.suggestTypeQuery)

  def suggestProperty(service:String,prefix:Option[String],type_strict:String,limit:Option[Int],start:Int,callback:Option[String]) = suggest(service,prefix,type_strict,limit,start,callback,(x)=>x.suggestPropertyQuery)

  def suggestEntity(service:String,prefix:Option[String],type_strict:String,limit:Option[Int],start:Int,callback:Option[String]) = suggest(service,prefix,type_strict,limit,start,callback,(x)=>x.suggestEntityQuery)

  def metadata()(implicit service : SPARQLReconcileEndpoint) : Either[JsValue,String] = {
    Left(service.writeMetadata()) 
  }

  def configuration(service:String) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service "+service+" doesn't exist")
    else Ok(service2.get.writeConfiguration())
  }
  
  def services() = Action {
    Ok(Json.toJson(serviceMap.map{ case (k,v)=>(k,v.name)}.toMap))
  }
  
  def configure(service:String) = Action { implicit request =>
    val jsonBody = request.body.asJson
    if (!jsonBody.isDefined) BadRequest("Expecting a JSON body\n")
    else try {
      val json = jsonBody.get
      serviceMap(service)=buildSPARQLReconcileEndpoint(service, json)
      val fw = new FileWriter("services/"+service)
      fw.write(json.toString())
      fw.close()
      Ok("Service "+service+" ("+(json \ "name")+") saved.\n")
    } catch {
      case e : Exception => BadRequest("Badly formatted request: missing "+e.getMessage)
    }
  }
  
  def delete(service:String) = Action {
    val service2 = serviceMap.remove(service)
    if (!service2.isDefined) NotFound("Service "+service+" doesn't exist")
    else {
      new File(servicesDir.getAbsolutePath+"/"+service).delete()
      Ok("Service "+service+" ("+service2.get.name+") deleted.\n")
    }
  }
  
  def flyout(service:String,url:Option[String],callback:Option[String],getFlyoutInfo:(SPARQLReconcileEndpoint)=>(String,Int,Int)) = Action { implicit request =>
    val service2 = serviceMap.get(service)
    if (!service2.isDefined) NotFound("Service "+service+" doesn't exist")
    var callback2 = callback
    var url2 = url
    val formBody = request.body.asFormUrlEncoded
    formBody.foreach { data =>
      url2 = data.get("url").map(_(0)).orElse(url)
      callback2 = data.get("callback").map(_(0)).orElse(callback)
    }
    val (flyoutURL,height,width) = getFlyoutInfo(service2.get)
    val r = Json.obj("html"->("<iframe class=\"fbs-flyoutpane\" height=\""+height+"\" width=\""+width+"\" src=\""+flyoutURL.replace("${id}",url2.get)+"\"></iframe>"))
    Ok(callback2.map { _ + "("+r.toString+")" }.getOrElse(r.toString()))
  }
  
  def typeFlyout(service:String,url:Option[String],callback:Option[String]) = {
    flyout(service,url,callback,(x:SPARQLReconcileEndpoint) => (x.typeFlyoutURL,x.typeFlyoutHeight,x.typeFlyoutWidth))
  }

  def propertyFlyout(service:String,url:Option[String],callback:Option[String]) = {
    flyout(service,url,callback,(x:SPARQLReconcileEndpoint) => (x.propertyFlyoutURL,x.propertyFlyoutHeight,x.propertyFlyoutWidth))
  }

  def entityFlyout(service:String,url:Option[String],callback:Option[String]) = {
    flyout(service,url,callback,(x:SPARQLReconcileEndpoint) => (x.entityFlyoutURL,x.entityFlyoutHeight,x.entityFlyoutWidth))
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
