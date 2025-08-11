# Integración de API y Base de Datos Raymond

![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Características](#características)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Requisitos Previos](#requisitos-previos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución de Pruebas](#ejecución-de-pruebas)
- [Ejecución de la Aplicación](#ejecución-de-la-aplicación)
- [Registro y Monitoreo](#registro-y-monitoreo)
- [Solución de Problemas](#solución-de-problemas)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)

## Descripción General

**Integración de API y Base de Datos Raymond** es una aplicación basada en Python diseñada para interactuar con una API externa y gestionar datos dentro de una base de datos MySQL. La aplicación extrae datos de la API, los valida e inserta en la base de datos manejando posibles errores de manera eficiente. Pruebas unitarias exhaustivas aseguran la fiabilidad y robustez de la aplicación.

## Características

- **Integración con API:** Obtiene datos desde un endpoint de API especificado.
- **Validación de Datos:** Garantiza que todos los campos requeridos estén presentes y correctamente formateados antes de procesar.
- **Operaciones de Base de Datos:** Inserta datos validados en tablas MySQL con manejo adecuado de errores.
- **Registro de Logs:** Registro detallado para monitorear el comportamiento de la aplicación y solucionar problemas.
- **Pruebas Unitarias:** Extensa suite de pruebas que cubren interacciones con la API y operaciones de base de datos.
- **Listo para Despliegue:** Configurable para despliegue como servicio o dentro de contenedores Docker.

## Estructura del Proyecto

```
Raymond/
├── README.md
├── app.log
├── picklist.py
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── client.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py
│   │   ├── models.py
│   │   └── operations.py
│   ├── main.py
│   └── utils/
│       ├── __init__.py
│       ├── helpers.py
│       └── logger.py
└── tests/
    ├── __init__.py
    ├── __pycache__/
    ├── test_api.py
    └── test_db.py
```

## Requisitos Previos

- **Python 3.12** o superior
- **MySQL Server** accesible con credenciales adecuadas
- **Git** para control de versiones
- **Herramientas de Entorno Virtual** (ejemplo: `venv`)

## Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/JuliMolinaZ/TotvsServices.git
cd Raymond
```

### 2. Crear un Entorno Virtual

```bash
python3 -m venv venv

# Activar el entorno virtual
# En macOS/Linux:
source venv/bin/activate

# En Windows:
venv\Scripts\activate
```

### 3. Instalar Dependencias

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuración

### 1. Variables de Entorno
La aplicación utiliza variables de entorno para la configuración, gestionadas a través de un archivo `.env`. Este archivo debe ubicarse en el directorio raíz del proyecto.

```bash
touch .env
```

## Ejecución de Pruebas

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Ejecución de la Aplicación

### 1. Navegar al Directorio Raíz del Proyecto

```bash
cd /ruta/al/proyecto/Raymond/
```

### 2. Activar el Entorno Virtual

```bash
# En macOS/Linux:
source venv/bin/activate

# En Windows:
venv\Scripts\activate
```

### 3. Ejecutar la Aplicación

```bash
python -m src.main
```

## Registro y Monitoreo

Los logs de la aplicación se encuentran en `app.log`. Para monitorear en tiempo real:

```bash
tail -f app.log
```

## Solución de Problemas

Si experimentas problemas, revisa los logs y asegúrate de que todas las dependencias estén correctamente instaladas.

## Contribuciones

Las contribuciones son bienvenidas. Por favor, abre un issue o envía un pull request.

## Licencia

Este proyecto está bajo la licencia MIT. Consulta el archivo `LICENSE` para más detalles.

