package com.crevasse.plugin.extension

interface DataFormatHandler extends Serializable {

    DataFormatType getDataFormatType()

    enum DataFormatType implements Serializable {
        ICEBERG
    }
}