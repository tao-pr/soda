package de.tao.soda

trait Help {

  def printHelp: Unit = {
    Console.println(
      """
        |Instruction
        |----------------------
        |soda-cli can be run with following arguments
        |-cp soda-cli -do:{CMD} -p:{PATH} -o:{OUTPUT}
        |""".stripMargin)
  }
}
