package br.uff.samba.web


val cinza = mutableListOf("#696969", "#808080", "#A9A9A9", "#C0C0C0")

val azul = mutableListOf("#6A5ACD", "#483D8B", "#191970", "#000080", "#00008B", "#0000CD", "#0000FF", "#6495ED", "#4169E1", "#1E90FF", "#00BFFF", "#87CEFA", "#ADD8E6", "#4682B4", "#B0C4DE", "#708090", "#778899")

val ciano = mutableListOf("#00FFFF", "#00CED1", "#40E0D0", "#48D1CC", "#20B2AA", "#008B8B", "#008B8B", "#7FFFD4", "#66CDAA", "#5F9EA0")

val verde = mutableListOf("#00FA9A", "#8FBC8F", "#3CB371", "#2E8B57", "#006400", "#228B22", "#32CD32", "#ADFF2F", "#9ACD32", "#6B8E23", "#556B2F", "#808000")

val marrom = mutableListOf("#BDB76B", "#DAA520", "#B8860B", "#8B4513", "#A0522D", "#BC8F8F", "#CD853F", "#D2691E", "#F4A460", "#D2B48C")

val roxo = mutableListOf("#7B68EE", "#9370DB", "#4B0082", "#BA55D3", "#800080")
val vermelho = mutableListOf("#8B0000", "#B22222", "#FA8072", "#E9967A", "#FF0000")

//    val all = mutableListOf(cinza, azul, vermelho, verde, marrom, roxo, ciano)
//
//    val size = all.map { it.size }.reduce { acc, i -> acc + i }
//
//    println("cores: " + size)
//
//    var count = 0
//
//    while (count < size - 1) {
//        for (i in 1..all.size - 1) {
//            if (all[i].isNotEmpty()) {
//                count++
//                println("\"${all[i].removeAt(0)}\"")
//            }
//        }
//    }
val allColors = listOf("#6A5ACD", "#00FA9A",
        "#BDB76B", "#00FFFF",
        "#B22222", "#8FBC8F", "#DAA520", "#9370DB",
        "#00CED1", "#191970", "#FA8072", "#3CB371",
        "#B8860B", "#4B0082", "#40E0D0", "#000080",
        "#E9967A", "#2E8B57", "#8B4513", "#BA55D3",
        "#48D1CC", "#00008B", "#FF0000", "#006400",
        "#A0522D", "#800080", "#20B2AA", "#0000CD",
        "#228B22", "#BC8F8F", "#008B8B", "#0000FF",
        "#32CD32", "#CD853F", "#008B8B", "#6495ED",
        "#ADFF2F", "#D2691E", "#7FFFD4", "#4169E1",
        "#9ACD32", "#F4A460", "#66CDAA", "#1E90FF",
        "#6B8E23", "#D2B48C", "#5F9EA0", "#00BFFF",
        "#556B2F", "#87CEFA", "#808000", "#ADD8E6",
        "#4682B4", "#B0C4DE", "#708090", "#778899")


