using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ArchivoAws
{
    /// <summary>
    /// Permite retornar la respuesta estandar del servicio.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ManejadorArchivoS3Response<T>
    {
        public string MensajeError { get; set; }
        public HttpStatusCode CodigoEstado { get; set; }
        public T EntidaRespuesta { get; set; }

        public ManejadorArchivoS3Response()
        {
            this.MensajeError = string.Empty;
        }

    }
}
