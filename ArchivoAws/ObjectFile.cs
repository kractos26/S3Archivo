using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ArchivoAws
{
    public class ObjectFile
    {
        /// <summary>
        /// Identificador del archivo.
        /// </summary>
        public string Clave { get; set; }
        public string Nombre { get; set; }

        public string Contenedor { get; set; }
       
        public string TipoContenido { get; set; }

        public Dictionary<string, string> MetaDatos { get; set; }

        public TypeMetadato MetadatoBusqueda { get; set; }
        public Stream ContenidoArchivo { get; set; }

        public string Prefijo { get; set; }
        public ObjectFile()
        {
            this.Clave = string.Empty;
            this.Nombre = string.Empty;
            this.Contenedor = string.Empty;
            this.TipoContenido = string.Empty;
            this.MetaDatos = new Dictionary<string, string>();
            this.Prefijo = string.Empty;
           
        }
    }
}
