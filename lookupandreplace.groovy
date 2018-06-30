import org.apache.nifi.controller.ControllerService
import java.nio.charset.StandardCharsets
import groovy.sql.Sql

// Executescript attributes
def serviceName = dbConnectionPoolName.value
def lookupTableName = lookupTable.value
def lookupColName = lookupColumn.value
def tagString = tag.value

// Some counters
def countChars = 0
def countReplaceExecutions = 0
def countFoundAllWords = 0


// get controller service lookup from context
def lookup = context.controllerServiceLookup
// search for serviceName in controller services
def dbcpServiceId = lookup.getControllerServiceIdentifiers(ControllerService).find {
    cs -> lookup.getControllerServiceName(cs) == serviceName
}
//Get the service from serviceid
def service = lookup.getControllerService(dbcpServiceId)
// Connect to service
def conn = service.getConnection()

def sql = new Sql(conn)
if (!sql) {
  log.error( "Failed to get SQL connection")
  conn?.close()
  return
}

if (!conn) {
  log.error( "Failed to connect to " + serviceName)
  return
}

def lookupCacheMap = [:]
def charsExpectedAfterMasking = 0

// Get the flowfile
def flowFile = session.get()
if(!flowFile) return


// Update flow file with outputStream
flowFile = session.write(flowFile, {inputStream, outputStream ->
        // Read line by line
        inputStream.eachLine { line ->
          // Get all first word character
          def matchedChars = line =~ /\b\w/

          // Create a temporary list to store
          // Sql where like statements
          def lookupChars = []
          def likeList = []
          matchedChars.each {
            // Remove any numbers
            if (! it.isNumber()) {
              // Add charcter to the lookup list
              lookupChars.add(it.toUpperCase())
              // Check if we already cached the lookup values
              if (!lookupCacheMap.containsKey(it.toUpperCase()))
              {
                // Add the new character and a list to lookup cache
                def newList = []
                lookupCacheMap[it.toUpperCase()] = newList

                // Convert to upper case
                // Add the new Like statement to the list
                likeList.add("${lookupColName} LIKE \"${it.toUpperCase()}%\"")
              }
            }
          }
          charsExpectedAfterMasking = matchedChars.size() - lookupChars.size()


          // Remove all duplicates
          lookupChars = lookupChars.unique()
          likeList = likeList.unique()
          def cacheList = []

          // Do we have more lookup values to cache?
          if (likeList.size() > 0) {
              // Join the list with OR statement
              def likeSql = likeList.join(' OR ')

              // Build the SQL lookup select statement
             def sqlCmd = "SELECT ${lookupColName} FROM ${lookupTableName} WHERE ${likeSql}"
             // Convert sqlStatement to string to secure compatibility with groovy sql
             def sqlCmdString = sqlCmd.toString()
            //def rows = sql.rows("SELECT fname FROM lookupTable WHERE fname LIKE 'M%' OR fname LIKE 'I%' OR fname LIKE 'N%' OR fname LIKE 'O%'")

             // execute sql and get the rows back
             def rows = sql.rows(sqlCmdString)
             // add all lines in lookupCache list
              // For each row mask any existence of the value in the line
              // (?i) is making the match case insenitive
              rows.each { row ->

                if (!lookupCacheMap.containsKey(row.address[0].toUpperCase()))
                {
                  // SQL sometimes return lines not requested
                  def newList = []
                  lookupCacheMap[row.address[0].toUpperCase()] = newList
                }
                // Add value to cache
                lookupCacheMap[row.address[0].toUpperCase()].add(row.address)

                //line = line.replaceAll("(?i)${row.address}", "${tagString}")
              }

            }
            countChars = countChars + lookupChars.size()


            // match your cache
            lookupChars.any{
              def replacedAllWords = false
              def charMapId = it
              lookupCacheMap[charMapId].any {

                  line = line.replaceAll("(?i)${it}", "<@@@@>")
                  countReplaceExecutions++


                  def matchedCharLeftInLine = line =~ /\b\w/


                  if (matchedCharLeftInLine.size() <= charsExpectedAfterMasking) {
                    countFoundAllWords++
                    if (replacedAllWords) {
                      log.error("### replacedAllWords true")
                    }
                    replacedAllWords = true
                    return true
                  }

              }
              if (replacedAllWords)
              {
                return true
              }

            }
            line = line.replaceAll("<@@@@>", "${tagString}") + "\n"


          // Write the result to outputStream
          outputStream.write(line.getBytes(StandardCharsets.UTF_8))
      }
  } as StreamCallback)

  //flowFile = session.putAttribute(flowFile, 'countLeftInLine', matchedCharLeftInLine.size().toString())
  // Write counters
  flowFile = session.putAttribute(flowFile, 'countChars', countChars.toString())
  flowFile = session.putAttribute(flowFile, 'countReplaceExecutions', countReplaceExecutions.toString())
  flowFile = session.putAttribute(flowFile, 'countLookupCacheMapChars', lookupCacheMap.size().toString())
  flowFile = session.putAttribute(flowFile, 'countFoundAllWords', countFoundAllWords.toString())

  session.transfer(flowFile, REL_SUCCESS)
// Release connection, this is important as it will otherwise block new executions
conn?.close()
