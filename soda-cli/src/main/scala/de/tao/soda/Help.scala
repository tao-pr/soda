package de.tao.soda

trait Help {

  def printHelp: Unit = {
    Console.println(
      """
        |Instruction
        |----------------------
        |soda-cli can be run with following arguments
        |-cp soda-cli -do:{CMD} -p:{PATH} -o:{OUTPUT} --debug
        |
        |Example usage:
        | (1) Download file
        |   -do:wget -p:https://domain.de/file.gz -o:local/path -i:dry,existok
        |
        |""".stripMargin)
  }
}
