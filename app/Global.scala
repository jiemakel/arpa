import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.GlobalSettings
import filters.CorsFilter

object Global extends WithFilters(new CorsFilter) with GlobalSettings